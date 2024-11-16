use std::env;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use avro_rs::types::{Record, Value};
use avro_rs::{Schema, Writer};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 取得輸入和輸出檔案路徑
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: {} <input_tsv> <output_avro>", args[0]);
        return Ok(());
    }
    let input_file = &args[1];
    let output_file = &args[2];

    // 定義 Avro Schema
    let raw_schema = r#"
    {
        "type": "record",
        "name": "SqlLog",
        "fields": [
            {"name": "conn_hash", "type": "string"},
            {"name": "stmt_id", "type": "int"},
            {"name": "exec_id", "type": "int"},
            {"name": "exec_time", "type": "string"},
            {"name": "sql_type", "type": "string"},
            {"name": "exe_status", "type": "string"},
            {"name": "db_ip", "type": "string"},
            {"name": "client_ip", "type": "string"},
            {"name": "client_host", "type": "string"},
            {"name": "app_name", "type": "string"},
            {"name": "db_user", "type": "string"},
            {"name": "sql_hash", "type": "string"},
            {"name": "from_tbs", "type": "string"},
            {"name": "select_cols", "type": "string"},
            {"name": "sql_stmt", "type": "string"},
            {"name": "stmt_bind_vars", "type": "string"}
        ]
    }
    "#;
    let schema = Schema::parse_str(raw_schema)?;

    // 打開輸入檔案
    let file = File::open(input_file)?;
    let reader = BufReader::new(file);

    // 創建 Avro Writer
    let mut writer = Writer::new(&schema, File::create(output_file)?);

    // 逐行讀取並解析 TSV
    for (line_num, line) in reader.lines().enumerate() {
        let line = line?;
        let columns: Vec<&str> = line.split('\t').collect();

        if columns.len() != 16 {
            eprintln!("Warning: Skipping malformed line {}: {}", line_num + 1, line);
            continue;
        }


        // 填充 Avro 記錄
        let mut record = Record::new(&schema).unwrap();
        record.put("conn_hash", columns[0].to_string());
        record.put("stmt_id", columns[1].parse::<i32>().unwrap_or_default()); // 使用預設值 0
        record.put("exec_id", columns[2].parse::<i32>().unwrap_or_default()); // 使用預設值 0
        record.put("exec_time", columns[3].to_string());
        record.put("sql_type", columns[4].to_string());
        record.put("exe_status", columns[5].to_string());
        record.put("db_ip", columns[6].to_string());
        record.put("client_ip", columns[7].to_string());
        record.put("client_host", columns[8].to_string());
        record.put("app_name", columns[9].to_string());
        record.put("db_user", columns[10].to_string());
        record.put("sql_hash", columns[11].to_string());
        record.put("from_tbs", columns[12].to_string());
        record.put("select_cols", columns[13].to_string());
        record.put("sql_stmt", columns[14].to_string());
        record.put("stmt_bind_vars", columns[15].to_string());


        // 將記錄寫入 Avro
        writer.append(record)?;
    }

    // 關閉 Avro Writer
    writer.flush()?;
    println!("Successfully converted {} to {}.", input_file, output_file);

    Ok(())
}

