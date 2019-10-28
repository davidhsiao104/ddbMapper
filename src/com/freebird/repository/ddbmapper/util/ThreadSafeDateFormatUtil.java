package com.freebird.repository.ddbmapper.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ThreadSafeDateFormatUtil {

  private String dateFormat;

  public ThreadSafeDateFormatUtil(String dateFormat) {
    super();
    this.dateFormat = dateFormat;
  }

  private ThreadLocal<DateFormat> df = new ThreadLocal<DateFormat>() {

    @Override
    public DateFormat get() {
      return super.get();
    }

    @Override
    protected DateFormat initialValue() {
      return new SimpleDateFormat(dateFormat);
    }

    @Override
    public void remove() {
      super.remove();
    }

    @Override
    public void set(DateFormat value) {
      super.set(value);
    }

  };

  public String format(Object date) {
    return df.get().format(date);
  }

  public Date parse(String strDate) throws ParseException {
    return df.get().parse(strDate);
  }

  public DateFormat getDateFormat() {
    return df.get();
  }
}
