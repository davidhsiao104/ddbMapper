package com.freebird.repository.ddbmapper;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.freebird.repository.ddbmapper.annotation.DDBAttr;
import com.freebird.repository.ddbmapper.annotation.DDBDocument;
import com.freebird.repository.ddbmapper.annotation.DDBHashKey;
import com.freebird.repository.ddbmapper.annotation.DDBIgnore;
import com.freebird.repository.ddbmapper.annotation.DDBRangeKey;
import com.freebird.repository.ddbmapper.annotation.DDBTable;
import com.freebird.repository.ddbmapper.util.ThreadSafeDateFormatUtil;

import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;

/**
 * 
 * @author david.hsiao
 *
 */
public class DDBMapper {

  // ISO-8601
  public final static ThreadSafeDateFormatUtil DATE_FORMATTER = new ThreadSafeDateFormatUtil("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

  public final static ThreadSafeDateFormatUtil DATE_MONTH_FORMATTER = new ThreadSafeDateFormatUtil("yyyyMM");

  public final static ThreadSafeDateFormatUtil DATE_DAY_FORMATTER = new ThreadSafeDateFormatUtil("yyyyMMdd");

  private final static long CUSTOMEPOCH = 1300000000000L;

  public final static int GET_MODE = 0;

  public final static int PUT_MODE = 1;

  public final static int UPDATE_MODE = 2;

  /**
   * 撠V�澆�‵entity - get �
   * 
   * @param entity
   * @param returnValue
   * @throws DDBModelException
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   * @throws ParseException
   * @throws ClassNotFoundException
   * @throws InstantiationException
   */
  public static void populateEntity(Object entity, Map<String, AttributeValue> returnValue) throws DDBModelException,
      IllegalArgumentException, IllegalAccessException, ParseException, ClassNotFoundException, InstantiationException {
    if (entity != null
        && (entity.getClass().isAnnotationPresent(DDBTable.class) || entity.getClass().isAnnotationPresent(DDBDocument.class))) {
      for (Field field : entity.getClass().getDeclaredFields()) {
        field.setAccessible(true);
        String key = DDBMapper.getByDDBAnnotationPresent(field);
        String typeName = field.getType().getCanonicalName();

        AttributeValue av = returnValue.get(key);

        if (av != null) {
          field.set(entity, DDBMapper.getAttributeValuePayload(typeName, av, field));
        }
      }
    } else {
      throw new DDBModelException("Entity no annotation present, like DDBTable or DDBDocument.");
    }
  }

  private static String getByDDBAnnotationPresent(Field field) {
    String name = field.getName();
    if (field.isAnnotationPresent(DDBAttr.class))
      name = field.getAnnotation(DDBAttr.class).name();
    else if (field.isAnnotationPresent(DDBHashKey.class))
      name = field.getAnnotation(DDBHashKey.class).name();
    else if (field.isAnnotationPresent(DDBRangeKey.class))
      name = field.getAnnotation(DDBRangeKey.class).name();

    return name;
  }

  /**
   * 靘����� AV �摰孵��
   * 
   * @param typeName
   * @param av
   * @param field
   * @return
   * @throws ParseException
   * @throws InstantiationException
   * @throws IllegalAccessException
   * @throws ClassNotFoundException
   * @throws IllegalArgumentException
   * @throws DDBModelException
   */
  private static Object getAttributeValuePayload(String typeName, AttributeValue av, Field field) throws ParseException,
      InstantiationException, IllegalAccessException, ClassNotFoundException, IllegalArgumentException, DDBModelException {
    Object obj = null;
    String thisTypeName = null;
    // ParameterizedType type = null;
    // Type value = null;
    if (av != null) {
      switch (typeName) {
        case "int":
        case "java.lang.Integer":
          if (av.n() != null)
            obj = new Integer(av.n());
          break;
        case "long":
        case "java.lang.Long":
          if (av.n() != null)
            obj = new Long(av.n());
          break;
        case "double":
        case "java.lang.Double":
          if (av.n() != null)
            obj = new Double(av.n());
          break;
        case "float":
        case "java.lang.Float":
          if (av.n() != null)
            obj = new Float(av.n());
          break;
        case "boolean":
        case "java.lang.Boolean":
          obj = av.bool();
          break;
        case "java.util.List":
          if (field != null) {
            ParameterizedType type = (ParameterizedType) field.getGenericType();
            Type value = type.getActualTypeArguments()[0];
            thisTypeName = value.getTypeName();
          } else {
            thisTypeName = "java.lang.String"; // 蝚砌�惜,撌脫��odel �霅鈭�
          }

          List newList = new ArrayList();
          for (AttributeValue avItem : av.l()) {
            newList.add(DDBMapper.getAttributeValuePayload(thisTypeName, avItem, null));
          }
          obj = newList;
          break;
        case "java.util.Map":
          if (field != null) {
            ParameterizedType type = (ParameterizedType) field.getGenericType();
            Type value = type.getActualTypeArguments()[1];
            thisTypeName = value.getTypeName();
          } else {
            thisTypeName = "java.lang.String"; // 蝚砌�惜,撌脫��odel �霅鈭�
          }

          Map<String, Object> newMap = new HashMap<String, Object>();
          for (Entry<String, AttributeValue> map : av.m().entrySet()) {
            newMap.put(map.getKey(), DDBMapper.getAttributeValuePayload(thisTypeName, map.getValue(), null));
          }
          obj = newMap;
          break;
        case "java.util.Date":
          if (av.s() != null && !av.s().equals(""))
            obj = DATE_FORMATTER.parse(av.s());
          break;
        case "java.lang.String":
          obj = av.s();
          break;
        default:
          Object newObj = Class.forName(typeName).newInstance();
          DDBMapper.populateEntity(newObj, av.m());
          obj = newObj;
          break;
      }

    }

    return obj;
  }

  /**
   * ������ttributeValueMap - insert �
   * 
   * @param entity
   * @param option PUT_MODE,UPDATE_MODE
   * @return
   * @throws DDBModelException
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   * @throws NOKeyException
   */
  public static DDBTableMeta extractEntityMeta(Object entity, int option)
      throws DDBModelException, IllegalArgumentException, IllegalAccessException, NOKeyException {
    if (entity != null && entity.getClass().isAnnotationPresent(DDBTable.class)) {
      DDBTableMeta meta = new DDBTableMeta();
      DDBTable table = entity.getClass().getAnnotation(DDBTable.class);
      meta.setTableName(table.name());

      for (Field field : entity.getClass().getDeclaredFields()) {
        String typeName = field.getType().getCanonicalName();
        if (field.isAnnotationPresent(DDBHashKey.class)) {
          field.setAccessible(true);
          DDBHashKey hashKey = field.getAnnotation(DDBHashKey.class);
          meta.setHashKeyName(hashKey.name());
          Object keyValue = field.get(entity);

          keyValue = keyGen(entity, option, field, hashKey.gen(), keyValue, null);

          meta.setHashKeyAttributeValue(DDBMapper.extractField(typeName, keyValue));
          meta.getAttributeMap().put(hashKey.name(), meta.getHashKeyAttributeValue());

        } else if (field.isAnnotationPresent(DDBRangeKey.class)) {
          field.setAccessible(true);
          DDBRangeKey rangeKey = field.getAnnotation(DDBRangeKey.class);
          meta.setRangeKeyName(rangeKey.name());
          Object keyValue = field.get(entity);

          keyValue = keyGen(entity, option, field, rangeKey.gen(), keyValue, rangeKey.prefix());

          AttributeValue av = DDBMapper.extractField(typeName, keyValue);
          if (av != null) {
            meta.setRangeKeyAttributeValue(av);
            meta.getAttributeMap().put(rangeKey.name(), av);
          } else if (rangeKey.required()) {
            throw new NOKeyException("NO Range Key");
          }
        } else if (!field.isAnnotationPresent(DDBIgnore.class)) {
          field.setAccessible(true);
          String key = null;
          boolean updateable = true;
          if (field.isAnnotationPresent(DDBAttr.class)) {
            DDBAttr attr = field.getAnnotation(DDBAttr.class);
            key = attr.name();
            updateable = attr.updateable();
          } else {
            key = field.getName();
          }

          if (option == PUT_MODE || option == GET_MODE)
            meta.getAttributeMap().put(key, DDBMapper.extractField(typeName, field.get(entity)));
          else if (option == UPDATE_MODE && updateable) // Key only in AttributeMap
            meta.getUpdatedAttributeMap().put(key, AttributeValueUpdate.builder()
                .value(DDBMapper.extractField(typeName, field.get(entity))).action(AttributeAction.PUT).build());
          else if (updateable)
            throw new DDBModelException("CRUD mode not correct.");

        }
      }

      return meta;
    } else {
      throw new DDBModelException("Entity no annotation present, like DDBTable.");
    }
  }

  private static Object keyGen(Object entity, int option, Field field, DDBHashKey.KEY_GEN keyGen, Object keyValue, String prefix)
      throws IllegalAccessException, NOKeyException {
    // gen. key
    if (option == PUT_MODE && keyValue == null && keyGen != DDBHashKey.KEY_GEN.NONE) {
      switch (keyGen) {
        case UUID:
          keyValue = UUID.randomUUID().toString();
          break;
        case NUM:
          keyValue = DDBMapper.generateRowId();
          break;
        case NUM_STR:
          keyValue = DDBMapper.generateRowId();
          keyValue = keyValue.toString();
          break;
        case MONTH:
          keyValue = DATE_MONTH_FORMATTER.format(new Date());
          break;
        case DAY:
          keyValue = DATE_DAY_FORMATTER.format(new Date());
          break;
        default:

      }

      if (prefix != null && !prefix.equals(""))
        keyValue = prefix + keyValue;

      field.set(entity, keyValue);
    } else if (keyValue == null) {
      throw new NOKeyException(field.getName() + "'s value is required.");
    }
    return keyValue;
  }

  // defined attribute type
  private static AttributeValue extractField(String typeName, Object v) {

    AttributeValue av = null;
    if (v != null) {

      switch (typeName) {
        case "int":
        case "java.lang.Integer":
        case "long":
        case "java.lang.Long":
        case "double":
        case "java.lang.Double":
        case "float":
        case "java.lang.Float":
          av = AttributeValue.builder().n(v.toString()).build();
          break;
        case "boolean":
        case "java.lang.Boolean":
          av = AttributeValue.builder().bool((Boolean) v).build();
          break;
        case "java.util.List":
        case "java.util.ArrayList":
          ArrayList newArr = new ArrayList();
          for (Object innerV : (List) v) {
            if (innerV != null) {
              String valueType = innerV.getClass().getCanonicalName();
              newArr.add(DDBMapper.extractField(valueType, innerV));
            }
          }
          av = AttributeValue.builder().l(newArr).build();
          break;
        case "java.util.Map":
          // Document type
          Map map = (Map) v;
          Map newMap = new HashMap<String, AttributeValue>();
          for (Iterator iterator = map.keySet().iterator(); iterator.hasNext();) {
            String key = (String) iterator.next();
            Object innerV = map.get(key);
            if (innerV != null) {
              String valueType = innerV.getClass().getCanonicalName();
              newMap.put(key, DDBMapper.extractField(valueType, innerV));
            }
          }

          av = AttributeValue.builder().m(newMap).build();
          break;
        case "java.util.Date":
          av = AttributeValue.builder().s(DATE_FORMATTER.format(v)).build();
          break;
        case "java.lang.String":
          av = AttributeValue.builder().s(v.toString()).build();
          break;
        default:
          ObjectMapper om = new ObjectMapper();
          om.setDateFormat(DATE_FORMATTER.getDateFormat());
          av = DDBMapper.extractField("java.util.Map", om.convertValue(v, Map.class));
          break;
      }

    }
    return av;
  }

  private static long generateRowId() {
    long ts = new Date().getTime() - CUSTOMEPOCH;
    double shardId = Math.floor(Math.random() * 64);
    double randid = Math.floor(Math.random() * 512);
    ts = (ts * 64); // bit-shift << 6
    ts = ts + (long) shardId;
    return (ts * 512) + (long) (randid % 512);

  }
}
