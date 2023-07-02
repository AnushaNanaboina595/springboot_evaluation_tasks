package com.dextrus_springboot_tasks.entity;

public class RequestBodyQuery {

	private ConnectionProperties properties;
	private String query;
	public ConnectionProperties getProperties() {
		return properties;
	}
	public void setProperties(ConnectionProperties properties) {
		this.properties = properties;
	}
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query;
	}
	
}
