package com.freebird.repository.ddbmapper;

import java.util.HashMap;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;

public class DDBTableMeta {

	public String tableName;

	private String hashKeyName;

	private String rangeKeyName;

	private AttributeValue hashKeyAttributeValue;

	private AttributeValue rangeKeyAttributeValue;

	private HashMap<String, AttributeValue> attributeMap = new HashMap<String, AttributeValue>();

	private HashMap<String, AttributeValueUpdate> updatedAttributeMap = new HashMap<String, AttributeValueUpdate>();

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getHashKeyName() {
		return hashKeyName;
	}

	public void setHashKeyName(String hashKeyName) {
		this.hashKeyName = hashKeyName;
	}

	public String getRangeKeyName() {
		return rangeKeyName;
	}

	public void setRangeKeyName(String rangeKeyName) {
		this.rangeKeyName = rangeKeyName;
	}

	public AttributeValue getHashKeyAttributeValue() {
		return hashKeyAttributeValue;
	}

	public void setHashKeyAttributeValue(AttributeValue hashKeyAttributeValue) {
		this.hashKeyAttributeValue = hashKeyAttributeValue;
	}

	public AttributeValue getRangeKeyAttributeValue() {
		return rangeKeyAttributeValue;
	}

	public void setRangeKeyAttributeValue(AttributeValue rangeKeyAttributeValue) {
		this.rangeKeyAttributeValue = rangeKeyAttributeValue;
	}

	public HashMap<String, AttributeValue> getAttributeMap() {
		return attributeMap;
	}

	public HashMap<String, AttributeValueUpdate> getUpdatedAttributeMap() {
		return updatedAttributeMap;
	}

}
