from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col, count, when, year, trim, sum
import os
import shutil

# 定义一个品牌映射字典，用于品牌归类
def map_brand_prefix(brand_name):
    brand_prefix_mapping = {
        'CT': 'Crucial',
        'DELLBOSS': 'Dell BOSS',
        'HGST': 'HGST',
        'Seagate': 'Seagate',
        'ST': 'Seagate',
        'TOSHIBA': 'Toshiba',
        'WDC': 'Western Digital'
    }

    for key, value in brand_prefix_mapping.items():
        if brand_name.startswith(key):
            return value
    return 'Others'


def return_yearly_failed_drives(input_folder, column_name, filter_value):
    spark = SparkSession.builder \
        .appName("CSVProcessing") \
        .getOrCreate()

    yearly_failure_results = []

    # 遍历 input_folder 下的所有子目录
    for sub_folder in os.listdir(input_folder):
        sub_folder_path = os.path.join(input_folder, sub_folder)

        if os.path.isdir(sub_folder_path):
            print(f"正在读取文件夹: {sub_folder}")

            # 遍历该子文件夹中的 所有CSV 文件
            for filename in os.listdir(sub_folder_path):
                    if filename.endswith('.csv'):
                        file_path = os.path.join(sub_folder_path, filename)
                        print(f"正在读取文件: {file_path}")
                        df = spark.read.csv(file_path, header=True, inferSchema=True)


                        # 创建年份列以及品牌列
                        df = df.withColumn("year", year(col('date')))
                        df = df.withColumn('brand', when(trim(col('model')).startswith('CT'), 'Crucial')
                                                    .when(trim(col('model')).startswith('DELLBOSS'), 'Dell BOSS')
                                                    .when(trim(col('model')).startswith('HGST'), 'HGST')
                                                    .when(trim(col('model')).startswith('Seagate'), 'Seagate')
                                                    .when(trim(col('model')).startswith('ST'), 'Seagate')
                                                    .when(trim(col('model')).startswith('TOSHIBA'), 'Toshiba')
                                                    .when(trim(col('model')).startswith('WDC'), 'Western Digital')
                                                    .otherwise('Others'))
                        # 按照year喝brand打组排序并且填加到列表
                        failure_stats = df.filter(df[column_name] == filter_value).groupBy('year', 'brand').agg(count(column_name).alias('failure_count')).orderBy('year', 'brand')
                        yearly_failure_results.append(failure_stats)

    # 输出处理后的daily相关数据文件到指定文件夹
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)

    # 创建输出文件夹
    output_temp_dir = os.path.join(parent_dir, "DATA_ENGINEERING_101_RESULTS/temp")

    # 输出处理后的yearly相关数据文件到指定文件夹
    if yearly_failure_results:
        final_df = yearly_failure_results[0]

        # 统计某年某个品牌文件的失败次数
        for i in range(1, len(yearly_failure_results)):
            final_df = final_df.union(yearly_failure_results[i])

        final_df = final_df.groupBy('year', 'brand').agg(sum('failure_count').alias('total_failure_count'))
        _final_output_dir = os.path.join(parent_dir, "DATA_ENGINEERING_101_RESULTS/yearly")

        if not os.path.exists(_final_output_dir):
            os.makedirs(_final_output_dir)

        # 写入yearly相关数据到表格
        final_df.write.csv(output_temp_dir, header=True, mode='overwrite')

        # 移动yearly相关数据到表格
        for filename in os.listdir(output_temp_dir):
            if filename.endswith('.csv'):
                _temp_file_path = os.path.join(output_temp_dir, filename)
                _final_output_path = os.path.join(_final_output_dir, f"{datetime.now().strftime('%Y-%m-%d')}.csv")
                
                shutil.move(_temp_file_path, _final_output_path)
                print(f"yearly文件输出路径: {_final_output_path}")
                break

    # 删除临时文件夹
    shutil.rmtree(output_temp_dir)
    spark.stop()


# 函数调用示例
# return_yearly_failed_drives("/Users/lulu.yu/Downloads/DATA_ENGINEERING_101", "failure", 1)
