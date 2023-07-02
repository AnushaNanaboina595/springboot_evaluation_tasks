package com.dextrus_springboot_tasks.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

import com.dextrus_springboot_tasks.common.CC;
import com.dextrus_springboot_tasks.entity.ConnectionProperties;
import com.dextrus_springboot_tasks.entity.TableDescription;
import com.dextrus_springboot_tasks.entity.TableType;

@Service
public class ConnectionService {

	public Connection getSQLServerConnection(ConnectionProperties properties) {
		Connection con = CC.getConnection(properties);
		return con;
	}

	public List<String> getCatalogsList(ConnectionProperties connectionProperties) {
		// TODO Auto-generated method stub
		List<String> catalogs = null;
		Connection connection = CC.getConnection(connectionProperties);
		try {
//			Statement statement=connection.createStatement();
//			ResultSet rs=statement.executeQuery("SELECT name FROM sys.databases");
			ResultSet rs = connection.createStatement().executeQuery("SELECT name FROM sys.databases");
			catalogs = new ArrayList<>();
			while (rs.next()) {
				catalogs.add(rs.getString("name"));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return catalogs;
	}

	public List<String> getSchemasList(ConnectionProperties properties, String catalog) {
		List<String> schemas = null;
		Connection connection = CC.getConnection(properties);
		try {
			Statement statement = connection.createStatement();
//			ResultSet resultSet=statement.executeQuery("use "+catalog+"; "+"SELECT name FROM sys.schemas");
			String query = "SELECT name FROM \"" + catalog + "\".sys.schemas";
			ResultSet rs = statement.executeQuery(query);
			schemas = new ArrayList<>();
			while (rs.next()) {
				schemas.add(rs.getString("name"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return schemas;
	}

	public List<TableType> getTablesAndViews(ConnectionProperties properties, String catalog, String schemaName) {
		List<TableType> viewsAndTables = new ArrayList<>();
		Connection con = CC.getConnection(properties);
		try {
			PreparedStatement statement = con.prepareStatement("use " + catalog + "; " + CC.GET_TABLES_QUERY);
			statement.setString(1, catalog);
			statement.setString(2, schemaName);
			ResultSet rs = statement.executeQuery();
			while (rs.next()) {
				TableType tableType = new TableType();
				tableType.setTableName(rs.getString("TABLE_NAME"));
				tableType.setTableType(rs.getString("TABLE_TYPE"));
				viewsAndTables.add(tableType);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return viewsAndTables;
	}

	public List<TableDescription> getTableDescription(ConnectionProperties properties, String catalog, String schema,
			String table) {
		List<TableDescription> tableDescList = new ArrayList<>();
		Connection con = CC.getConnection(properties);
		try {
			PreparedStatement statement = con.prepareStatement("use " + catalog + "; " + CC.DESCRIPTION_QUERY);
			table = schema + "." + table;
			statement.setString(1, table);
			ResultSet rs = statement.executeQuery();
			while (rs.next()) {
				TableDescription td = new TableDescription();
				td.setColumnName(rs.getString("COLUMN_NAME"));
				td.setDataType(rs.getString("DATA_TYPE"));
				td.setIsNullable(rs.getInt("IS_NULLABLE"));
				td.setMaxlength(rs.getInt("MAX_LENGTH"));
				td.setPrecision(rs.getInt("PRECISION"));
				td.setPrimaryKey(rs.getInt("PRIMARY_KEY"));
				tableDescList.add(td);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return tableDescList;
	}

	public List<List<Object>> getTableData(ConnectionProperties prop, String query) {
		List<List<Object>> rows = new ArrayList<>();

		Connection con = CC.getConnection(prop);
		try {
			Statement statement = con.createStatement();
			ResultSet rs = statement.executeQuery(query);
			ResultSetMetaData metadata = rs.getMetaData();
			int columnCount = metadata.getColumnCount();
			while (rs.next()) {
				List<Object> row = new ArrayList<>();
				for (int i = 1; i <= columnCount; i++) {
					String columnName = metadata.getColumnName(i);
					String columnType = metadata.getColumnTypeName(i);

					switch (columnType) {

					case "varchar": {
						row.add(columnName + " : " + rs.getString(columnName));
						break;
					}
					case "float": {
						row.add(columnName + " : " + rs.getFloat(columnName));
						break;
					}
					case "boolean": {
						row.add(columnName + " : " + rs.getBoolean(columnName));
						break;
					}
					case "int": {
						row.add(columnName + " : " + rs.getInt(columnName));
						break;
					}
					case "timestamp": {
						row.add(columnName + " : " + rs.getTimestamp(columnName));
						break;
					}
					case "decimal": {
						row.add(columnName + " : " + rs.getBigDecimal(columnName));
						break;
					}
					case "date": {
						row.add(columnName + " : " + rs.getDate(columnName));
						break;
					}
					default:
						row.add("!-!-! " + columnName + " : " + rs.getObject(columnName));
						System.out.println("Datatype Not available for Column: " + columnName);
					}
				}
				rows.add(row);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return rows;
	}

	public List<TableType> getTablesAndViewsByPattern(ConnectionProperties properties, String catalog, String pattern) {
		List<TableType> viewsAndTables=new ArrayList<>();
		Connection connection=CC.getConnection(properties);
		try {
			PreparedStatement statement=connection.prepareStatement("use "+catalog+"; "+CC.GET_TABLES_BY_PATTERN_QUERY);
			statement.setString(1, pattern);
			ResultSet rs=statement.executeQuery();
			while(rs.next()) {
				TableType tableType = new TableType(); 
				tableType.setTableName("");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	public List<Map<String, Object>> getCountRowsFromTable(ConnectionProperties properties, String catalog,
			String schema, String table, int count, Class<?> pojoClass) {
		List<Map<String, Object>> rows = new ArrayList<>();
		try {
			Connection con = CC.getConnection(properties);
			Statement statement = con.createStatement();
			String query = "use " + catalog + "; SELECT TOP " + count + " * FROM " + schema + "." + table;
			ResultSet rs = statement.executeQuery(query);
			ResultSetMetaData meta = rs.getMetaData();
			int columnCount = meta.getColumnCount();
			while (rs.next()) {
				Map<String, Object> row = new HashMap<>();
				for (int i = 1; i <= columnCount; i++) {
					String columnName = meta.getColumnName(i);
					String propertyName = columnNameToPropertyName(columnName);
					Object value = rs.getObject(i);
					row.put(propertyName, value);
				}
				rows.add(row);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rows;
	}

//Helper method to convert column names to property names
	private static String columnNameToPropertyName(String columnName) {
		String[] parts = columnName.split("_");
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < parts.length; i++) {
			sb.append(capitalize(parts[i]));
		}
		return sb.toString();
	}

//Helper method to capitalize the first letter of a string
	private static String capitalize(String s) {
		return s.substring(0, 1).toUpperCase() + s.substring(1);
	}

}
