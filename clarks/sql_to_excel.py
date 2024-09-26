from db_config import DbConfig
import pandas as pd
import datetime

obj = DbConfig()
#
qr = f'SELECT * FROM {obj.data_table}'
obj.cur.execute(qr)
results = obj.cur.fetchall()
df = pd.read_sql(qr, obj.con)
date_today = datetime.datetime.today()
date_today_strf = date_today.strftime("%d_%m_%Y")

output_file = f'clarks_{date_today_strf}.xlsx'

# Create a pandas Excel writer object using XlsxWriter as the engine
with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
    # Write the DataFrame to the Excel file
    df.to_excel(writer, index=False, sheet_name='Sheet1')

    # Access the XlsxWriter workbook and worksheet objects
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    #
    # # Define cell formats
    # number_format = workbook.add_format({'num_format': '#,##0.00'})  # For formatting numbers with two decimal places
    # bold_format = workbook.add_format({'bold': True})
    #
    # # Apply formatting to the entire column
    # worksheet.set_column('B:B', 18, number_format)  # Format 'Price' column
    # worksheet.set_column('C:C', 18, number_format)  # Format 'Discount' column
    #
    # # Set the header format
    header_format = workbook.add_format({'bold': True, 'bg_color': '#DCE6F1', 'border': 1})
    for col_num, value in enumerate(df.columns):
        worksheet.write(0, col_num, value, header_format)

print(f"Data exported and formatted successfully to {output_file}")
