/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene5_shaded.document;


import org.apache.lucene5_shaded.search.NumericRangeQuery; // for javadocs
import org.apache.lucene5_shaded.search.PrefixQuery;
import org.apache.lucene5_shaded.search.TermRangeQuery;
import org.apache.lucene5_shaded.util.NumericUtils;        // for javadocs

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Provides support for converting dates to strings and vice-versa.
 * The strings are structured so that lexicographic sorting orders 
 * them by date, which makes them suitable for use as field values 
 * and search terms.
 * 
 * <P>This class also helps you to limit the resolution of your dates. Do not
 * save dates with a finer resolution than you really need, as then
 * {@link TermRangeQuery} and {@link PrefixQuery} will require more memory and become slower.
 * 
 * <P>
 * Another approach is {@link NumericUtils}, which provides
 * a sortable binary representation (prefix encoded) of numeric values, which
 * date/time are.
 * For indexing a {@link Date} or {@link Calendar}, just get the unix timestamp as
 * <code>long</code> using {@link Date#getTime} or {@link Calendar#getTimeInMillis} and
 * index this as a numeric value with {@link LongField}
 * and use {@link NumericRangeQuery} to query it.
 */
public class DateTools {
  
  final static TimeZone GMT = TimeZone.getTimeZone("GMT");

  private static final ThreadLocal<Calendar> TL_CAL = new ThreadLocal<Calendar>() {
    @Override
    protected Calendar initialValue() {
      return Calendar.getInstance(GMT, Locale.ROOT);
    }
  };

  //indexed by format length
  private static final ThreadLocal<SimpleDateFormat[]> TL_FORMATS = new ThreadLocal<SimpleDateFormat[]>() {
    @Override
    protected SimpleDateFormat[] initialValue() {
      SimpleDateFormat[] arr = new SimpleDateFormat[Resolution.MILLISECOND.formatLen+1];
      for (Resolution resolution : Resolution.values()) {
        arr[resolution.formatLen] = (SimpleDateFormat)resolution.format.clone();
      }
      return arr;
    }
  };

  // cannot create, the class has static methods only
  private DateTools() {}

  /**
   * Converts a Date to a string suitable for indexing.
   * 
   * @param date the date to be converted
   * @param resolution the desired resolution, see
   *  {@link #round(Date, Resolution)}
   * @return a string in format <code>yyyyMMddHHmmssSSS</code> or shorter,
   *  depending on <code>resolution</code>; using GMT as timezone 
   */
  public static String dateToString(Date date, Resolution resolution) {
    return timeToString(date.getTime(), resolution);
  }

  /**
   * Converts a millisecond time to a string suitable for indexing.
   * 
   * @param time the date expressed as milliseconds since January 1, 1970, 00:00:00 GMT
   * @param resolution the desired resolution, see
   *  {@link #round(long, Resolution)}
   * @return a string in format <code>yyyyMMddHHmmssSSS</code> or shorter,
   *  depending on <code>resolution</code>; using GMT as timezone
   */
  public static String timeToString(long time, Resolution resolution) {
    final Date date = new Date(round(time, resolution));
    return TL_FORMATS.get()[resolution.formatLen].format(date);
  }
  
  /**
   * Converts a string produced by <code>timeToString</code> or
   * <code>dateToString</code> back to a time, represented as the
   * number of milliseconds since January 1, 1970, 00:00:00 GMT.
   * 
   * @param dateString the date string to be converted
   * @return the number of milliseconds since January 1, 1970, 00:00:00 GMT
   * @throws ParseException if <code>dateString</code> is not in the 
   *  expected format 
   */
  public static long stringToTime(String dateString) throws ParseException {
    return stringToDate(dateString).getTime();
  }

  /**
   * Converts a string produced by <code>timeToString</code> or
   * <code>dateToString</code> back to a time, represented as a
   * Date object.
   * 
   * @param dateString the date string to be converted
   * @return the parsed time as a Date object 
   * @throws ParseException if <code>dateString</code> is not in the 
   *  expected format 
   */
  public static Date stringToDate(String dateString) throws ParseException {
    try {
      return TL_FORMATS.get()[dateString.length()].parse(dateString);
    } catch (Exception e) {
      throw new ParseException("Input is not a valid date string: " + dateString, 0);
    }
  }
  
  /**
   * Limit a date's resolution. For example, the date <code>2004-09-21 13:50:11</code>
   * will be changed to <code>2004-09-01 00:00:00</code> when using
   * <code>Resolution.MONTH</code>. 
   * 
   * @param resolution The desired resolution of the date to be returned
   * @return the date with all values more precise than <code>resolution</code>
   *  set to 0 or 1
   */
  public static Date round(Date date, Resolution resolution) {
    return new Date(round(date.getTime(), resolution));
  }
  
  /**
   * Limit a date's resolution. For example, the date <code>1095767411000</code>
   * (which represents 2004-09-21 13:50:11) will be changed to 
   * <code>1093989600000</code> (2004-09-01 00:00:00) when using
   * <code>Resolution.MONTH</code>.
   * 
   * @param resolution The desired resolution of the date to be returned
   * @return the date with all values more precise than <code>resolution</code>
   *  set to 0 or 1, expressed as milliseconds since January 1, 1970, 00:00:00 GMT
   */
  @SuppressWarnings("fallthrough")
  public static long round(long time, Resolution resolution) {
    final Calendar calInstance = TL_CAL.get();
    calInstance.setTimeInMillis(time);
    
    switch (resolution) {
      //NOTE: switch statement fall-through is deliberate
      case YEAR:
        calInstance.set(Calendar.MONTH, 0);
      case MONTH:
        calInstance.set(Calendar.DAY_OF_MONTH, 1);
      case DAY:
        calInstance.set(Calendar.HOUR_OF_DAY, 0);
      case HOUR:
        calInstance.set(Calendar.MINUTE, 0);
      case MINUTE:
        calInstance.set(Calendar.SECOND, 0);
      case SECOND:
        calInstance.set(Calendar.MILLISECOND, 0);
      case MILLISECOND:
        // don't cut off anything
        break;
      default:
        throw new IllegalArgumentException("unknown resolution " + resolution);
    }
    return calInstance.getTimeInMillis();
  }

  /** Specifies the time granularity. */
  public static enum Resolution {
    
    /** Limit a date's resolution to year granularity. */
    YEAR(4), 
    /** Limit a date's resolution to month granularity. */
    MONTH(6), 
    /** Limit a date's resolution to day granularity. */
    DAY(8), 
    /** Limit a date's resolution to hour granularity. */
    HOUR(10), 
    /** Limit a date's resolution to minute granularity. */
    MINUTE(12), 
    /** Limit a date's resolution to second granularity. */
    SECOND(14), 
    /** Limit a date's resolution to millisecond granularity. */
    MILLISECOND(17);

    final int formatLen;
    final SimpleDateFormat format;//should be cloned before use, since it's not threadsafe

    Resolution(int formatLen) {
      this.formatLen = formatLen;
      // formatLen 10's place:                     11111111
      // formatLen  1's place:            12345678901234567
      this.format = new SimpleDateFormat("yyyyMMddHHmmssSSS".substring(0,formatLen),Locale.ROOT);
      this.format.setTimeZone(GMT);
    }

    /** this method returns the name of the resolution
     * in lowercase (for backwards compatibility) */
    @Override
    public String toString() {
      return super.toString().toLowerCase(Locale.ROOT);
    }

  }

}
