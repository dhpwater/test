package com.ns.hive.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class HiveUtil {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	
	private static Connection con = null ;
	
	public static Connection getCon(){
		if (con == null){
			try {
				Class.forName(driverName);
				try {
					con = DriverManager.getConnection("jdbc:hive2://bsa180:10000/internal_app_bsatat", "", "");
					return con;
				} catch (SQLException e) {
					e.printStackTrace();
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			
		}
		return con;
	}
	
	public static void destroyCon(){
		if(con != null){
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
