/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.whitelist;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.JodaCompatibleZonedDateTime;
import org.elasticsearch.xpack.ql.expression.function.scalar.whitelist.InternalQlScriptUtils;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateAddProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateDiffProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DatePartProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeFormatProcessor.Formatter;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeParseProcessor.Parser;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTruncProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.NamedDateTimeProcessor.NameExtractor;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.NonIsoDateTimeProcessor.NonIsoDateTimeExtractor;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.QuarterProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.TimeProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.GeoProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.StDistanceProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.StWkttosqlProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.BinaryMathProcessor.BinaryMathOperation;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.BinaryOptionalMathProcessor.BinaryOptionalMathOperation;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.BinaryStringNumericProcessor.BinaryStringNumericOperation;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.BinaryStringStringProcessor.BinaryStringStringOperation;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.ConcatFunctionProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.InsertFunctionProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.LocateFunctionProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.ReplaceFunctionProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.StringProcessor.StringOperation;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.SubstringFunctionProcessor;
import org.elasticsearch.xpack.sql.expression.literal.geo.GeoShape;
import org.elasticsearch.xpack.sql.expression.literal.interval.IntervalDayTime;
import org.elasticsearch.xpack.sql.expression.literal.interval.IntervalYearMonth;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.ConditionalProcessor.ConditionalOperation;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.NullIfProcessor;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.SqlBinaryArithmeticOperation;
import org.elasticsearch.xpack.sql.type.SqlDataTypeConverter;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.time.Duration;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

/**
 * Whitelisted class for SQL scripts.
 * Acts as a registry of the various static methods used <b>internally</b> by the scalar functions
 * (to simplify the whitelist definition).
 */
@SuppressWarnings("unused")
public class InternalSqlScriptUtils extends InternalQlScriptUtils {

    InternalSqlScriptUtils() {}

    //
    // Conditional
    //
    public static Object coalesce(List<Object> expressions) {
        return ConditionalOperation.COALESCE.apply(expressions);
    }

    public static Object greatest(List<Object> expressions) {
        return ConditionalOperation.GREATEST.apply(expressions);
    }

    public static Object least(List<Object> expressions) {
        return ConditionalOperation.LEAST.apply(expressions);
    }

    public static Object nullif(Object left, Object right) {
        return NullIfProcessor.apply(left, right);
    }

    //
    // Math
    //
    public static Object add(Object left, Object right) {
        return SqlBinaryArithmeticOperation.ADD.apply(left, right);
    }

    public static Object div(Object left, Object right) {
        return SqlBinaryArithmeticOperation.DIV.apply(left, right);
    }

    public static Object mod(Object left, Object right) {
        return SqlBinaryArithmeticOperation.MOD.apply(left, right);
    }

    public static Object mul(Object left, Object right) {
        return SqlBinaryArithmeticOperation.MUL.apply(left, right);
    }

    public static Object sub(Object left, Object right) {
        return SqlBinaryArithmeticOperation.SUB.apply(left, right);
    }

    public static Number round(Number v, Number s) {
        return BinaryOptionalMathOperation.ROUND.apply(v, s);
    }

    public static Number truncate(Number v, Number s) {
        return BinaryOptionalMathOperation.TRUNCATE.apply(v, s);
    }

    public static Number abs(Number value) {
        return MathOperation.ABS.apply(value);
    }

    public static Number acos(Number value) {
        return MathOperation.ACOS.apply(value);
    }

    public static Number asin(Number value) {
        return MathOperation.ASIN.apply(value);
    }

    public static Number atan(Number value) {
        return MathOperation.ATAN.apply(value);
    }

    public static Number atan2(Number left, Number right) {
        return BinaryMathOperation.ATAN2.apply(left, right);
    }

    public static Number cbrt(Number value) {
        return MathOperation.CBRT.apply(value);
    }

    public static Number ceil(Number value) {
        return MathOperation.CEIL.apply(value);
    }

    public static Number cos(Number value) {
        return MathOperation.COS.apply(value);
    }

    public static Number cosh(Number value) {
        return MathOperation.COSH.apply(value);
    }

    public static Number cot(Number value) {
        return MathOperation.COT.apply(value);
    }

    public static Number degrees(Number value) {
        return MathOperation.DEGREES.apply(value);
    }

    public static Number e(Number value) {
        return MathOperation.E.apply(value);
    }

    public static Number exp(Number value) {
        return MathOperation.EXP.apply(value);
    }

    public static Number expm1(Number value) {
        return MathOperation.EXPM1.apply(value);
    }

    public static Number floor(Number value) {
        return MathOperation.FLOOR.apply(value);
    }

    public static Number log(Number value) {
        return MathOperation.LOG.apply(value);
    }

    public static Number log10(Number value) {
        return MathOperation.LOG10.apply(value);
    }

    public static Number pi(Number value) {
        return MathOperation.PI.apply(value);
    }

    public static Number power(Number left, Number right) {
        return BinaryMathOperation.POWER.apply(left, right);
    }

    public static Number radians(Number value) {
        return MathOperation.RADIANS.apply(value);
    }

    public static Number random(Number value) {
        return MathOperation.RANDOM.apply(value);
    }

    public static Number sign(Number value) {
        return MathOperation.SIGN.apply(value);
    }

    public static Number sin(Number value) {
        return MathOperation.SIN.apply(value);
    }

    public static Number sinh(Number value) {
        return MathOperation.SINH.apply(value);
    }

    public static Number sqrt(Number value) {
        return MathOperation.SQRT.apply(value);
    }

    public static Number tan(Number value) {
        return MathOperation.TAN.apply(value);
    }
    
    

    //
    // Date/Time functions
    //    
    @Deprecated
    public static Integer dateTimeChrono(Object dateTime, String tzId, String chronoName) {
        String extractorName = null;
        switch (chronoName) {
            case "DAY_OF_WEEK":
                extractorName = "ISO_DAY_OF_WEEK";
                break;
            case "ALIGNED_WEEK_OF_YEAR":
                extractorName = "ISO_WEEK_OF_YEAR";
                break;
            default:
                extractorName = chronoName;
        }
        return dateTimeExtract(dateTime, tzId, extractorName);
    }
    
    public static Integer dateTimeExtract(Object dateTime, String tzId, String extractorName) {
        if (dateTime == null || tzId == null || extractorName == null) {
            return null;
        }
        if (dateTime instanceof OffsetTime) {
            return TimeProcessor.doProcess((OffsetTime) dateTime, tzId, extractorName);
        }
        return DateTimeProcessor.doProcess(asDateTime(dateTime), tzId, extractorName);
    }

    public static String dayName(Object dateTime, String tzId) {
        if (dateTime == null || tzId == null) {
            return null;
        }
        return NameExtractor.DAY_NAME.extract(asDateTime(dateTime), tzId);
    }

    public static Integer dayOfWeek(Object dateTime, String tzId) {
        if (dateTime == null || tzId == null) {
            return null;
        }
        return NonIsoDateTimeExtractor.DAY_OF_WEEK.extract(asDateTime(dateTime), tzId);
    }

    public static String monthName(Object dateTime, String tzId) {
        if (dateTime == null || tzId == null) {
            return null;
        }
        return NameExtractor.MONTH_NAME.extract(asDateTime(dateTime), tzId);
    }

    public static Integer quarter(Object dateTime, String tzId) {
        if (dateTime == null || tzId == null) {
            return null;
        }
        return QuarterProcessor.quarter(asDateTime(dateTime), tzId);
    }

    public static Integer weekOfYear(Object dateTime, String tzId) {
        if (dateTime == null || tzId == null) {
            return null;
        }
        return NonIsoDateTimeExtractor.WEEK_OF_YEAR.extract(asDateTime(dateTime), tzId);
    }

    public static ZonedDateTime dateAdd(String dateField, Integer numberOfUnits, Object dateTime, String tzId) {
        return (ZonedDateTime) DateAddProcessor.process(dateField, numberOfUnits, asDateTime(dateTime) , ZoneId.of(tzId));
    }

    public static Integer dateDiff(String dateField, Object dateTime1, Object dateTime2, String tzId) {
        return (Integer) DateDiffProcessor.process(dateField, asDateTime(dateTime1), asDateTime(dateTime2) , ZoneId.of(tzId));
    }

    public static Object dateTrunc(String truncateTo, Object dateTimeOrInterval, String tzId) {
        if (dateTimeOrInterval instanceof IntervalDayTime || dateTimeOrInterval instanceof IntervalYearMonth) {
           return DateTruncProcessor.process(truncateTo, dateTimeOrInterval, ZoneId.of(tzId));
        }
        return DateTruncProcessor.process(truncateTo, asDateTime(dateTimeOrInterval), ZoneId.of(tzId));
    }

    public static Object dateParse(String dateField, String pattern, String tzId) {
        return Parser.DATE.parse(dateField, pattern, ZoneId.of(tzId));
    }

    public static Integer datePart(String dateField, Object dateTime, String tzId) {
        return (Integer) DatePartProcessor.process(dateField, asDateTime(dateTime), ZoneId.of(tzId));
    }

    public static String dateTimeFormat(Object dateTime, String pattern, String tzId) {
        return (String) Formatter.DATE_TIME_FORMAT.format(asDateTime(dateTime), pattern, ZoneId.of(tzId));
    }

    public static Object dateTimeParse(String dateField, String pattern, String tzId) {
        return Parser.DATE_TIME.parse(dateField, pattern, ZoneId.of(tzId));
    }

    public static String format(Object dateTime, String pattern, String tzId) {
        return (String) Formatter.FORMAT.format(asDateTime(dateTime), pattern, ZoneId.of(tzId));
    }

    public static String toChar(Object dateTime, String pattern, String tzId) {
        return (String) Formatter.TO_CHAR.format(asDateTime(dateTime), pattern, ZoneId.of(tzId));
    }

    public static Object timeParse(String dateField, String pattern, String tzId) {
        return Parser.TIME.parse(dateField, pattern, ZoneId.of(tzId));
    }

    public static ZonedDateTime asDateTime(Object dateTime) {
        return (ZonedDateTime) asDateTime(dateTime, false);
    }

    private static Object asDateTime(Object dateTime, boolean lenient) {
        if (dateTime == null) {
            return null;
        }
        if (dateTime instanceof JodaCompatibleZonedDateTime) {
            return ((JodaCompatibleZonedDateTime) dateTime).getZonedDateTime();
        }
        if (dateTime instanceof ZonedDateTime) {
            return dateTime;
        }
        if (false == lenient) {
            if (dateTime instanceof Number) {
                return DateUtils.asDateTimeWithMillis(((Number) dateTime).longValue());
            }

            if (dateTime instanceof String) {
                return DateUtils.asDateTimeWithNanos(dateTime.toString());
            }
            throw new SqlIllegalArgumentException("Invalid date encountered [{}]", dateTime);
        }
        return dateTime;
    }

    public static IntervalDayTime intervalDayTime(String text, String typeName) {
        if (text == null || typeName == null) {
            return null;
        }
        return new IntervalDayTime(Duration.parse(text), SqlDataTypes.fromSqlOrEsType(typeName));
    }

    public static IntervalYearMonth intervalYearMonth(String text, String typeName) {
        if (text == null || typeName == null) {
            return null;
        }

        return new IntervalYearMonth(Period.parse(text), SqlDataTypes.fromSqlOrEsType(typeName));
    }

    public static OffsetTime asTime(String time) {
        return OffsetTime.parse(time);
    }

    //
    // String functions
    //
    public static Integer ascii(String s) {
        return (Integer) StringOperation.ASCII.apply(s);
    }

    public static Integer bitLength(String s) {
        return (Integer) StringOperation.BIT_LENGTH.apply(s);
    }

    public static String character(Number n) {
        return (String) StringOperation.CHAR.apply(n);
    }

    public static Integer charLength(String s) {
        return (Integer) StringOperation.CHAR_LENGTH.apply(s);
    }

    public static String concat(String s1, String s2) {
        return (String) ConcatFunctionProcessor.process(s1, s2);
    }

    public static String insert(String s, Number start, Number length, String r) {
        return (String) InsertFunctionProcessor.doProcess(s, start, length, r);
    }

    public static String lcase(String s) {
        return (String) StringOperation.LCASE.apply(s);
    }

    public static String left(String s, Number count) {
        return BinaryStringNumericOperation.LEFT.apply(s, count);
    }

    public static Integer length(String s) {
        return (Integer) StringOperation.LENGTH.apply(s);
    }

    public static Integer locate(String s1, String s2) {
        return locate(s1, s2, null);
    }

    public static Integer locate(String s1, String s2, Number pos) {
        return LocateFunctionProcessor.doProcess(s1, s2, pos);
    }

    public static String ltrim(String s) {
        return (String) StringOperation.LTRIM.apply(s);
    }

    public static Integer octetLength(String s) {
        return (Integer) StringOperation.OCTET_LENGTH.apply(s);
    }

    public static Integer position(String s1, String s2) {
        return (Integer) BinaryStringStringOperation.POSITION.apply(s1, s2);
    }

    public static String repeat(String s, Number count) {
        return BinaryStringNumericOperation.REPEAT.apply(s, count);
    }

    public static String replace(String s1, String s2, String s3) {
        return (String) ReplaceFunctionProcessor.doProcess(s1, s2, s3);
    }

    public static String right(String s, Number count) {
        return BinaryStringNumericOperation.RIGHT.apply(s, count);
    }

    public static String rtrim(String s) {
        return (String) StringOperation.RTRIM.apply(s);
    }

    public static String space(Number n) {
        return (String) StringOperation.SPACE.apply(n);
    }

    public static String substring(String s, Number start, Number length) {
        return (String) SubstringFunctionProcessor.doProcess(s, start, length);
    }

    public static String trim(String s) {
        return (String) StringOperation.TRIM.apply(s);
    }

    public static String ucase(String s) {
        return (String) StringOperation.UCASE.apply(s);
    }

    public static String stAswkt(Object v) {
        return GeoProcessor.GeoOperation.ASWKT.apply(v).toString();
    }

    public static GeoShape stWktToSql(String wktString) {
        return StWkttosqlProcessor.apply(wktString);
    }

    public static Double stDistance(Object v1, Object v2) {
        return StDistanceProcessor.process(v1, v2);
    }

    public static String stGeometryType(Object g) {
        return (String) GeoProcessor.GeoOperation.GEOMETRY_TYPE.apply(g);
    }

    public static Double stX(Object g) {
        return (Double) GeoProcessor.GeoOperation.X.apply(g);
    }

    public static Double stY(Object g) {
        return (Double) GeoProcessor.GeoOperation.Y.apply(g);
    }

    public static Double stZ(Object g) {
        return (Double) GeoProcessor.GeoOperation.Z.apply(g);
    }

    // processes doc value as a geometry
    public static <T> GeoShape geoDocValue(Map<String, ScriptDocValues<T>> doc, String fieldName) {
        Object obj = docValue(doc, fieldName);
        if (obj != null) {
            if (obj instanceof GeoPoint) {
                return new GeoShape(((GeoPoint) obj).getLon(), ((GeoPoint) obj).getLat());
            }
            // TODO: Add support for geo_shapes when it is there
        }
        return null;
    }

    //
    // Casting
    //
    public static Object cast(Object value, String typeName) {
        // we call asDateTime here to make sure we handle JodaCompatibleZonedDateTime properly,
        // since casting works for ZonedDateTime objects only
        return SqlDataTypeConverter.convert(asDateTime(value, true), SqlDataTypes.fromSqlOrEsType(typeName));
    }
}
