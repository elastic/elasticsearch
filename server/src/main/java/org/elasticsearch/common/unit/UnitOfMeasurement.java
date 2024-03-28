/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.unit;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.core.TimeValue;

import java.util.concurrent.TimeUnit;

public interface UnitOfMeasurement {

    /**
     * Creates a unit of measurement instance given a unit identifier that preferably should be a
     * case-sensitive <a href="http://unitsofmeasure.org/ucum.html">UCUM symbol</a>.
     *
     * @param unit the unit identifier, preferably a case-sensitive UCUM symbol
     * @return either a well-known unit of measurement that can do conversions within it's scale or a generic unknown unit
     */
    static UnitOfMeasurement of(String unit) {
        if (unit == null) {
            return Unknown.NONE;
        }
        // because UCUM identifiers are unique, we can identify both the scale and the unit
        // thus, there's no need for a separate parameter such as scale=time
        // also supports some non-UCUM identifiers (such as byte) for convenience and compatibility reasons
        // when the risk of collisions with actual UCUM units is very low
        return switch (unit) {
            case "ns" -> new Time(TimeUnit.NANOSECONDS, unit);
            case "us" -> new Time(TimeUnit.MICROSECONDS, unit);
            case "ms" -> new Time(TimeUnit.MILLISECONDS, unit);
            case "s" -> new Time(TimeUnit.SECONDS, unit);
            case "min" -> new Time(TimeUnit.MINUTES, unit);
            case "h" -> new Time(TimeUnit.HOURS, unit);
            case "d" -> new Time(TimeUnit.DAYS, unit);
            case "By", "byte" -> new Byte(unit);
            case "%", "percent" -> new Percent(unit);
            default -> new Unknown(unit);
        };
    }

    /**
     * If the provided value is a {@link String} or {@link BytesRef} with a unit suffix, the value will be converted to this unit.
     * If the provided value has a different type or doesn't contain a unit suffix, the original value is returned.
     *
     * @param value the value to convert, which may be a {@link String}, a {@link BytesRef} containing a string, or a {@link Number}
     * @param fieldName the name of the field to convert. Used in error messages.
     * @return the converted or the original value, depending on whether the provided value contains a unit suffix
     * @throws org.elasticsearch.ElasticsearchParseException if the provided value contains a unit suffix but the value can't be parsed
     */
    Object tryConvert(Object value, String fieldName);

    String unit();

    abstract class AbstractUnitOfMeasurement implements UnitOfMeasurement {

        private final String unit;

        protected AbstractUnitOfMeasurement(String unit) {
            this.unit = unit;
        }

        public Object tryConvert(Object value, String fieldName) {
            if (value instanceof BytesRef bValue) {
                value = bValue.utf8ToString();
            }
            if (value instanceof String sValue) {
                String trimmed = sValue.trim();
                if (hasUnitSuffix(trimmed)) {
                    try {
                        return doConvert(trimmed, fieldName);
                    } catch (IllegalArgumentException e) {
                        throw new ElasticsearchParseException(e.getMessage(), e);
                    }
                }
            }
            return value;
        }

        private boolean hasUnitSuffix(String sValue) {
            return Character.isDigit(sValue.charAt(sValue.length() - 1)) == false;
        }

        abstract Number doConvert(String value, String fieldName);

        @Override
        public String unit() {
            return unit;
        }
    }

    class Unknown implements UnitOfMeasurement {

        private static final Unknown NONE = new Unknown(null);
        private final String unit;

        private Unknown(String unit) {
            this.unit = unit;
        }

        @Override
        public Object tryConvert(Object value, String fieldName) {
            return value;
        }

        @Override
        public String unit() {
            return unit;
        }
    }

    class Time extends AbstractUnitOfMeasurement {

        private final TimeUnit timeUnit;

        Time(TimeUnit timeUnit, String unit) {
            super(unit);
            this.timeUnit = timeUnit;
        }

        @Override
        public Number doConvert(String value, String fieldName) {
            TimeValue timeValue = TimeValue.parseTimeValue(value, fieldName);
            return timeUnit.convert(timeValue.duration(), timeValue.timeUnit());
        }
    }

    class Byte extends AbstractUnitOfMeasurement {

        Byte(String unit) {
            super(unit);
        }

        @Override
        public Number doConvert(String value, String fieldName) {
            return ByteSizeValue.parseBytesSizeValue(value, fieldName).getBytes();
        }
    }

    class Percent extends AbstractUnitOfMeasurement {

        protected Percent(String unit) {
            super(unit);
        }

        @Override
        Number doConvert(String value, String fieldName) {
            return RatioValue.parseRatioValue(value).getAsRatio();
        }
    }
}
