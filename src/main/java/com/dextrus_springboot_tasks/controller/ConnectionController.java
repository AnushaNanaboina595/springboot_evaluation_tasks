package com.dextrus_springboot_tasks.controller;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.dextrus_springboot_tasks.entity.ConnectionProperties;
import com.dextrus_springboot_tasks.entity.RequestBodyPattern;
import com.dextrus_springboot_tasks.entity.RequestBodyQuery;
import com.dextrus_springboot_tasks.entity.TableDescription;
import com.dextrus_springboot_tasks.entity.TableType;
import com.dextrus_springboot_tasks.service.ConnectionService;
import com.fasterxml.jackson.core.io.UTF8Writer;

import jakarta.websocket.server.PathParam;

@RestController
@RequestMapping("/dextrus")
public class ConnectionController {

	@Autowired
	private ConnectionService service;

//	Create an API that takes connections details as input and makes test connection and returns the status in JSON
	@PostMapping("/connect")
	public ResponseEntity<String> connectToSqlserver(@RequestBody ConnectionProperties properties) {
		Connection connection = service.getSQLServerConnection(properties);
		if (connection == null)
			return new ResponseEntity<String>("Connection Failed", HttpStatus.SERVICE_UNAVAILABLE);
		else
			return new ResponseEntity<String>("Connected to SQL Server", HttpStatus.OK);
	}

//	Create an API that takes connections details as input and returns the list of the catalogs in the database as JSON response
	@PostMapping("/catalogs")
	public ResponseEntity<List<String>> getCatalogs(@RequestBody ConnectionProperties connectionProperties){
		List<String> catalogs=service.getCatalogsList(connectionProperties);
		return new ResponseEntity<List<String>>(catalogs, HttpStatus.OK);
	}

//	 Create an API that takes connections details and catalog name as input and returns the list of all the schemas in a given catalog as JSON response
	@PostMapping("/{catalog:.+}")
	public ResponseEntity<List<String>> getSchemas(@RequestBody ConnectionProperties properties, @PathVariable("catalog") String catalog){
		String catalogName = null;
		try {
			catalogName=URLDecoder.decode(catalog,"UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		List<String> schemas=service.getSchemasList(properties,catalog);
		return new ResponseEntity<List<String>>(schemas,HttpStatus.OK);
	}
	
//	Create an API that takes connections details and catalog name, Schema name as input list of all the views and tables in a schema
	@PostMapping("/{catalog}/{schema}")
	public ResponseEntity<List<TableType>> getViewsAndTables(@RequestBody ConnectionProperties properties, 
	                                                         @PathParam(value = "catalog") String catalog, 
	                                                         @PathParam(value = "schema") String schemaName) {
	    List<TableType> viewsAndTables = service.getTablesAndViews(properties, catalog, schemaName);
	    return new ResponseEntity<>(viewsAndTables, HttpStatus.OK);
	}

//	Create an API that takes connections details and catalog name, Schema Name, Table Name, and list of all the columns and properties of a table/view as JSON response
	@PostMapping("/{catalog}/{schema}/{table}")
	public ResponseEntity<List<TableDescription>> getColumnProperties(@RequestBody ConnectionProperties properties, @PathVariable String catalog, @PathVariable String schema, @PathVariable String table){
		List<TableDescription> tableDescList=service.getTableDescription(properties,catalog,schema,table);
		return new ResponseEntity<List<TableDescription>>(tableDescList,HttpStatus.OK);
	}
	
//	Create an API that takes connections details and a sample query and returns the metadata and the data as JSON response
//	row is a collection of colmns data so List of object and here data means collect of rows so List<List<Object>>
	@PostMapping("/query")
	public ResponseEntity<List<List<Object>>> getTableData(@RequestBody RequestBodyQuery queryBody){
		ConnectionProperties prop = queryBody.getProperties();
		String query = queryBody.getQuery();
		List<List<Object>> tableDataList = service.getTableData(prop, query);
		return new ResponseEntity<List<List<Object>>>(tableDataList, HttpStatus.OK);
	}
	
//	Create an API that takes connections details and passes on a search string and returns all the list of tables and views that matches with a string across the catalog and schema
	
	@PostMapping("/search")
	public ResponseEntity<List<TableType>> getTablesByPattern(@RequestBody RequestBodyPattern bodyPattern){
		List<TableType> viewsAndTables = service.getTablesAndViewsByPattern(bodyPattern.getProperties(),bodyPattern.getCatalog(),bodyPattern.getPattern());
		return new ResponseEntity<List<TableType>>(viewsAndTables, HttpStatus.OK);
	}
	
// 
	
	@PostMapping("/{catalog}/{schema}/{table}/{count}")
	public ResponseEntity<List<Map<String, Object>>> getCountRowsFromTable(@RequestBody ConnectionProperties properties,@PathVariable String catalog, @PathVariable String schema, @PathVariable String table,@PathVariable int count){
		List<Map<String, Object>> tableData = service.getCountRowsFromTable(properties,catalog,schema,table,count, null);
		return new ResponseEntity<List<Map<String,Object>>>(tableData,HttpStatus.OK);
	}
	
}



