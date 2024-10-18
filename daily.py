from pyspark.sql import SparkSession
from pyspark.sql import Row
from datetime import datetime
import os
import shutil

def return_daily_drives_and_failed_drives(input_folder, column_name, filter_value):
    spark = SparkSession.builder \
        .appName("CSVProcessing") \
        .getOrCreate()

    results = []

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

                        # 统计每天所有drive的总数目以及失败的drive数目
                        total_count = df.count()
                        filtered_df = df.filter(df[column_name] == filter_value)
                        failure_count = filtered_df.count()
                        
                        # 填加到列表
                        results.append(Row(file_name=filename[:-4], failure_count=failure_count, total_count=total_count))

    # 输出处理后的daily相关数据文件到指定文件夹
    result_df = spark.createDataFrame(results)
    result_df = result_df.orderBy('file_name')
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)

    # 创建输出文件夹
    output_temp_dir = os.path.join(parent_dir, "DATA_ENGINEERING_101_RESULTS/temp")
    final_output_dir = os.path.join(parent_dir, "DATA_ENGINEERING_101_RESULTS/daily")

    if not os.path.exists(final_output_dir):
        os.makedirs(final_output_dir)

    if not os.path.exists(final_output_dir):
        os.makedirs(output_temp_dir)


    # 写入daily相关数据到表格
    result_df.write.csv(output_temp_dir, header=True, mode='overwrite')

    # 移动daily输出文件到指定文件夹
    for filename in os.listdir(output_temp_dir):
        if filename.endswith('.csv'):
            temp_file_path = os.path.join(output_temp_dir, filename)
            final_output_path = os.path.join(final_output_dir, f"{datetime.now().strftime('%Y-%m-%d')}.csv")
            
            shutil.move(temp_file_path, final_output_path)
            print(f"daily文件输出路径: {final_output_path}")
            break

    # 删除临时文件夹
    shutil.rmtree(output_temp_dir)
    spark.stop()

# 函数调用示例
return_daily_drives_and_failed_drives("/Users/lulu.yu/Downloads/DATA_ENGINEERING_102", "failure", 1)
