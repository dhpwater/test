package com.ns.util;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;

public class HttpClient {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.out.println("test case");
		
		List<String> lists = null;
        try {  
            if(!(new File("persons.txt").exists())){
            	System.exit(0);
            }
            lists = FileUtils.readLines(new File("persons.txt"), "utf-8");
        } catch (IOException e) {  
             System.out.println(e);
        }  
        
        System.out.println(lists);
	}

}
