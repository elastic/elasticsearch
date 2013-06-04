package org.elasticsearch.common.joda;

import java.util.Locale;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.Strings;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.ISODateTimeFormat;

class PatternMatcher {

  private static final Map<String, DateTimeFormatter>  MAPPINGS = ImmutableMap.<String, DateTimeFormatter>builder()
      .put("basicDate", ISODateTimeFormat.basicDate())
      .put("basic_date", ISODateTimeFormat.basicDate())
      .put("basicDateTime", ISODateTimeFormat.basicDateTime())
      .put("basic_date_time", ISODateTimeFormat.basicDateTime())
      .put("basicDateTimeNoMillis", ISODateTimeFormat.basicDateTimeNoMillis())
      .put("basic_date_time_no_millis", ISODateTimeFormat.basicDateTimeNoMillis())
      .put("basicOrdinalDate", ISODateTimeFormat.basicOrdinalDate())
      .put("basic_ordinal_date", ISODateTimeFormat.basicOrdinalDate())
      .put("basicOrdinalDateTime", ISODateTimeFormat.basicOrdinalDateTime())
      .put("basic_ordinal_date_time", ISODateTimeFormat.basicOrdinalDateTime())
      .put("basicOrdinalDateTimeNoMillis", ISODateTimeFormat.basicOrdinalDateTimeNoMillis())
      .put("basic_ordinal_date_time_no_millis", ISODateTimeFormat.basicOrdinalDateTimeNoMillis())
      .put("basicTime", ISODateTimeFormat.basicTime())
      .put("basic_time", ISODateTimeFormat.basicTime())
      .put("basicTimeNoMillis", ISODateTimeFormat.basicTimeNoMillis())
      .put("basic_time_no_millis", ISODateTimeFormat.basicTimeNoMillis())
      .put("basicTTime", ISODateTimeFormat.basicTTime())
      .put("basic_t_Time", ISODateTimeFormat.basicTTime())
      .put("basicTTimeNoMillis", ISODateTimeFormat.basicTTimeNoMillis())
      .put("basic_t_time_no_millis", ISODateTimeFormat.basicTTimeNoMillis())
      .put("basicWeekDate", ISODateTimeFormat.basicWeekDate())
      .put("basic_week_date", ISODateTimeFormat.basicWeekDate())
      .put("basicWeekDateTime", ISODateTimeFormat.basicWeekDateTime())
      .put("basic_week_date_time", ISODateTimeFormat.basicWeekDateTime())
      .put("basicWeekDateTimeNoMillis", ISODateTimeFormat.basicWeekDateTimeNoMillis())
      .put("basic_week_date_time_no_millis", ISODateTimeFormat.basicWeekDateTimeNoMillis())
      .put("date", ISODateTimeFormat.date())
      .put("dateHour", ISODateTimeFormat.dateHour())
      .put("date_hour", ISODateTimeFormat.dateHour())
      .put("dateHourMinute", ISODateTimeFormat.dateHourMinute())
      .put("date_hour_minute", ISODateTimeFormat.dateHourMinute())
      .put("dateHourMinuteSecond", ISODateTimeFormat.dateHourMinuteSecond())
      .put("date_hour_minute_second", ISODateTimeFormat.dateHourMinuteSecond())
      .put("dateHourMinuteSecondFraction", ISODateTimeFormat.dateHourMinuteSecondFraction())
      .put("date_hour_minute_second_fraction", ISODateTimeFormat.dateHourMinuteSecondFraction())
      .put("dateHourMinuteSecondMillis", ISODateTimeFormat.dateHourMinuteSecondMillis())
      .put("date_hour_minute_second_millis", ISODateTimeFormat.dateHourMinuteSecondMillis())
      .put("dateTime", ISODateTimeFormat.dateTime())
      .put("date_time", ISODateTimeFormat.dateTime())
      .put("dateTimeNoMillis", ISODateTimeFormat.dateTimeNoMillis())
      .put("date_time_no_millis", ISODateTimeFormat.dateTimeNoMillis())
      .put("hour", ISODateTimeFormat.hour())
      .put("hourMinute", ISODateTimeFormat.hourMinute())
      .put("hour_minute", ISODateTimeFormat.hourMinute())
      .put("hourMinuteSecond", ISODateTimeFormat.hourMinuteSecond())
      .put("hour_minute_second", ISODateTimeFormat.hourMinuteSecond())
      .put("hourMinuteSecondFraction", ISODateTimeFormat.hourMinuteSecondFraction())
      .put("hour_minute_second_fraction", ISODateTimeFormat.hourMinuteSecondFraction())
      .put("hourMinuteSecondMillis", ISODateTimeFormat.hourMinuteSecondMillis())
      .put("hour_minute_second_millis", ISODateTimeFormat.hourMinuteSecondMillis())
      .put("ordinalDate", ISODateTimeFormat.ordinalDate())
      .put("ordinal_date", ISODateTimeFormat.ordinalDate())
      .put("ordinalDateTime", ISODateTimeFormat.ordinalDateTime())
      .put("ordinal_date_time", ISODateTimeFormat.ordinalDateTime())
      .put("ordinalDateTimeNoMillis", ISODateTimeFormat.ordinalDateTimeNoMillis())
      .put("ordinal_date_time_no_millis", ISODateTimeFormat.ordinalDateTimeNoMillis())
      .put("time", ISODateTimeFormat.time())
      .put("tTime", ISODateTimeFormat.tTime())
      .put("t_time", ISODateTimeFormat.tTime())
      .put("tTimeNoMillis", ISODateTimeFormat.tTimeNoMillis())
      .put("t_time_no_millis",ISODateTimeFormat.tTimeNoMillis())
      .put("weekDate", ISODateTimeFormat.weekDate())
      .put("week_date", ISODateTimeFormat.weekDate())
      .put("weekDateTime", ISODateTimeFormat.weekDateTime())
      .put("week_date_time", ISODateTimeFormat.weekDateTime())
      .put("weekyear", ISODateTimeFormat.weekyear())
      .put("week_year", ISODateTimeFormat.weekyear())
      .put("weekyearWeek", ISODateTimeFormat.weekyearWeek())
      .put("year", ISODateTimeFormat.year())
      .put("yearMonth", ISODateTimeFormat.yearMonth())
      .put("year_month", ISODateTimeFormat.yearMonth())
      .put("yearMonthDay", ISODateTimeFormat.yearMonthDay())
      .put("year_month_day", ISODateTimeFormat.yearMonthDay()).build();


    public FormatDateTimeFormatter getDateTimeFormatter(String input, Locale locale) {
      if ("dateOptionalTime".equals(input) || "date_optional_time".equals(input)) {
        // in this case, we have a separate parser and printer since the dataOptionalTimeParser can't print
        // this sucks we should use the root local by default and not be dependent on the node
        return new FormatDateTimeFormatter(input,
          ISODateTimeFormat.dateOptionalTimeParser().withZone(DateTimeZone.UTC),
          ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC), locale);
      } else {
        DateTimeFormatter formatter = MAPPINGS.get(input);
        if (formatter == null) {
          String[] formats = Strings.delimitedListToStringArray(input, "||");
          if (formats == null || formats.length == 1) {
            formatter = DateTimeFormat.forPattern(input);
          } else {
            DateTimeParser[] parsers = new DateTimeParser[formats.length];
            for (int i = 0; i < formats.length; i++) {
              parsers[i] = DateTimeFormat.forPattern(formats[i]).withZone(DateTimeZone.UTC).getParser();
            }
            DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder()
                .append(DateTimeFormat.forPattern(formats[0]).withZone(DateTimeZone.UTC).getPrinter(), parsers);
            formatter = builder.toFormatter();
          }
        }
        return new FormatDateTimeFormatter(input, formatter.withZone(DateTimeZone.UTC), locale);
      }
    }

}
