package com.ns.hive.data;

import org.apache.log4j.Logger;

public class HiveDataTest {
	
	private static Logger logger = Logger.getLogger(HiveDataTest.class);

	/**
	 * 向flow_info 表打入Parquet 数据
	 * 
	 * 新建orc 表
	 * 新建text 表
	 * 新建 carbondata 表
	 * @param args
	 */
	public static void main(String[] args){
		
		HiveService service = new HiveService();
		
		//获取flow_info表字段
		String fieldStr = service.getFieldStr();
		
		logger.info(fieldStr);
		
		service.createParquetTable(fieldStr);
		
		service.createORCTable(fieldStr);
		
		service.createTxtTable(fieldStr);
		
		service.loadDataParquetTable();
		
		service.loadDataORCTable();
		
		service.loadDataTextTable();
	}
}
