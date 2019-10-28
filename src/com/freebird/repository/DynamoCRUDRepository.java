package com.freebird.repository;


import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import com.freebird.repository.ddbmapper.DDBMapper;
import com.freebird.repository.ddbmapper.DDBModelException;
import com.freebird.repository.ddbmapper.DDBTableMeta;
import com.freebird.repository.ddbmapper.NOKeyException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public abstract class DynamoCRUDRepository<T> {

	@Autowired
	private DynamoDbClient ddb;

	public DynamoDbClient getDynamoDbClient() {
		return ddb;
	}

	public T getItem(T t) throws IllegalArgumentException, IllegalAccessException, DDBModelException, NOKeyException,
			InstantiationException, ClassNotFoundException, ParseException {
		DDBTableMeta meta = DDBMapper.extractEntityMeta(t, DDBMapper.GET_MODE);

		HashMap<String, AttributeValue> attributeMap = new HashMap<String, AttributeValue>();
		attributeMap.put(meta.getHashKeyName(), meta.getHashKeyAttributeValue());
		if (meta.getRangeKeyName() != null)
			attributeMap.put(meta.getRangeKeyName(), meta.getRangeKeyAttributeValue());

		GetItemRequest request = GetItemRequest.builder().key(attributeMap).tableName(meta.getTableName()).build();
		Map<String, AttributeValue> returnMap = ddb.getItem(request).item();
		if (returnMap != null && !returnMap.keySet().isEmpty()) {
			T newT = (T) t.getClass().newInstance();
			DDBMapper.populateEntity(newT, returnMap);
			return newT;
		}
		return null;
	}

	public List<T> queryByRangeKey(T t) throws IllegalArgumentException, IllegalAccessException, DDBModelException,
			NOKeyException, InstantiationException, ClassNotFoundException, ParseException {

		DDBTableMeta meta = DDBMapper.extractEntityMeta(t, DDBMapper.GET_MODE);

		HashMap<String, AttributeValue> attrValue = new HashMap<String, AttributeValue>();
		attrValue.put(":pk", meta.getHashKeyAttributeValue());
		attrValue.put(":typeRange", meta.getRangeKeyAttributeValue());

		QueryRequest queryReq = QueryRequest.builder().tableName(meta.getTableName())
				.keyConditionExpression("pk = :pk and begins_with(typeRange, :typeRange)")
				.expressionAttributeValues(attrValue).build();

		List<Map<String, AttributeValue>> returnMap = ddb.query(queryReq).items();
		List<T> retNewListT = new ArrayList<T>();
		for (Map<String, AttributeValue> map : returnMap) {
			T newT = (T) t.getClass().newInstance();
			DDBMapper.populateEntity(newT, map);
			retNewListT.add(newT);
		}
		return retNewListT;
	}

	public T saveItem(T t) throws IllegalArgumentException, IllegalAccessException, DDBModelException, NOKeyException {
		DDBTableMeta meta = DDBMapper.extractEntityMeta(t, DDBMapper.PUT_MODE);
		HashMap<String, AttributeValue> attributeMap = meta.getAttributeMap();

		PutItemRequest request = PutItemRequest.builder().tableName(meta.getTableName()).item(attributeMap).build();
		ddb.putItem(request);
		return t;
	}

	public T updateItem(T t)
			throws IllegalArgumentException, IllegalAccessException, DDBModelException, NOKeyException {
		DDBTableMeta meta = DDBMapper.extractEntityMeta(t, DDBMapper.UPDATE_MODE);

		UpdateItemRequest request = UpdateItemRequest.builder().tableName(meta.getTableName())
				.key(meta.getAttributeMap()).attributeUpdates(meta.getUpdatedAttributeMap()).build();

		ddb.updateItem(request);
		return t;
	}

	public int deleteItem(T t)
			throws IllegalArgumentException, IllegalAccessException, DDBModelException, NOKeyException {
		DDBTableMeta meta = DDBMapper.extractEntityMeta(t, DDBMapper.UPDATE_MODE);

		DeleteItemRequest deleteReq = DeleteItemRequest.builder().tableName(meta.getTableName())
				.key(meta.getAttributeMap()).build();

		ddb.deleteItem(deleteReq);
		return 1;
	}

	protected List<Map<String, AttributeValue>> batchGetPer100Item(String tableName,
			List<Map<String, AttributeValue>> keyItem) {

		List<Map<String, AttributeValue>> totalResponseMap = new ArrayList<Map<String, AttributeValue>>();
		List<Map<String, AttributeValue>> box = new ArrayList<Map<String, AttributeValue>>();
		int counter = 0;
		for (Map<String, AttributeValue> one : keyItem) {			
			box.add(one);
			counter++;
			if (counter == 100) {
				Map<String, KeysAndAttributes> requestItems = new HashMap<>();
				requestItems.put(tableName, KeysAndAttributes.builder().keys(keyItem).build());
				BatchGetItemRequest request = BatchGetItemRequest.builder().requestItems(requestItems).build();
				List<Map<String, AttributeValue>> responseMap = ddb.batchGetItem(request).responses().get(tableName);
				totalResponseMap.addAll(responseMap);
				box.clear();
				counter = 0;
			}
		}
		
		if(!box.isEmpty()) {
			Map<String, KeysAndAttributes> requestItems = new HashMap<>();
			requestItems.put(tableName, KeysAndAttributes.builder().keys(keyItem).build());
			BatchGetItemRequest request = BatchGetItemRequest.builder().requestItems(requestItems).build();
			List<Map<String, AttributeValue>> responseMap = ddb.batchGetItem(request).responses().get(tableName);
			totalResponseMap.addAll(responseMap);
		}

		return totalResponseMap;
	}
	
	protected void batchWritePer25Item(String tableName,
			List<WriteRequest> keyItem) {
		int counter = 0;
		List<WriteRequest> box = new ArrayList<WriteRequest>();
		for (WriteRequest one : keyItem) {
			box.add(one);
			counter++;
			if (counter == 25) {
				HashMap<String, List<WriteRequest>> map = new HashMap<String, List<WriteRequest>>();
			    map.put(tableName, box);			    
			    BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder().requestItems(map).build();
			    ddb.batchWriteItem(batchWriteItemRequest);				
				box.clear();
				counter = 0;
			}
		}
		if(!box.isEmpty()) {
			HashMap<String, List<WriteRequest>> map = new HashMap<String, List<WriteRequest>>();
		    map.put(tableName, box);			    
		    BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder().requestItems(map).build();
		    ddb.batchWriteItem(batchWriteItemRequest);	
		}
	}
}
