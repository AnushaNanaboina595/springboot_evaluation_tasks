package com.dextrus_springboot_tasks.entity;

public class RequestBodyPattern {
	private ConnectionProperties properties;
	private String pattern;
	private String catalog;
	public ConnectionProperties getProperties() {
		return properties;
	}
	public void setProperties(ConnectionProperties properties) {
		this.properties = properties;
	}
	public String getPattern() {
		return pattern;
	}
	public void setPattern(String pattern) {
		this.pattern = pattern;
	}
	public String getCatalog() {
		return catalog;
	}
	public void setCatalog(String catalog) {
		this.catalog = catalog;
	}
	
}
