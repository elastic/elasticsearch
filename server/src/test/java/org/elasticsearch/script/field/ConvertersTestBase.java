/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * A set of the standard available field types for scripting with raw values
 * contained in a test field to test conversions to other field types.
 */
public abstract class ConvertersTestBase extends ESTestCase {

    protected boolean[] rawBooleanValues;
    protected FieldValues<Boolean> booleanFieldValues;
    protected Field<Boolean> booleanField;

    @Before
    public void setupBooleanField() {
        rawBooleanValues = new boolean[] {
                true,
                false
        };

        booleanFieldValues = new FieldValues<Boolean>() {
            @Override
            public boolean isEmpty() {
                return false;
            }

            @Override
            public int size() {
                return rawBooleanValues.length;
            }

            @Override
            public List<Boolean> getValues() {
                List<Boolean> values = new ArrayList<>();

                for (boolean bool : rawBooleanValues) {
                    values.add(bool);
                }

                return values;
            }

            @Override
            public Boolean getNonPrimitiveValue() {
                return rawBooleanValues[0];
            }

            @Override
            public long getLongValue() {
                return BooleanField.toLong(rawBooleanValues[0]);
            }

            @Override
            public double getDoubleValue() {
                return BooleanField.toDouble(rawBooleanValues[0]);
            }
        };

        booleanField = new BooleanField("boolean_field", booleanFieldValues);
    }

    protected long[] rawLongValues;
    protected FieldValues<Long> longFieldValues;
    protected Field<Long> longField;

    @Before
    public void setupLongField() {
        rawLongValues = new long[] {
                15,
                -1,
                0,
                1,
                Long.MIN_VALUE,
                Long.MAX_VALUE,
                Integer.MIN_VALUE,
                Integer.MAX_VALUE
        };

        longFieldValues = new FieldValues<Long>() {
            @Override
            public boolean isEmpty() {
                return false;
            }

            @Override
            public int size() {
                return rawLongValues.length;
            }

            @Override
            public List<Long> getValues() {
                return LongStream.of(rawLongValues).boxed().collect(Collectors.toList());
            }

            @Override
            public Long getNonPrimitiveValue() {
                return rawLongValues[0];
            }

            @Override
            public long getLongValue() {
                return rawLongValues[0];
            }

            @Override
            public double getDoubleValue() {
                return rawLongValues[0];
            }
        };

        longField = new LongField("long_field", longFieldValues);
    }

    protected double[] rawDoubleValues;
    protected FieldValues<Double> doubleFieldValues;
    protected Field<Double> doubleField;

    @Before
    public void setupDoubleField() {
        rawDoubleValues = new double[] {
                3.456,
                0.0,
                -1.0,
                1.0,
                -1.5,
                1.5,
                Double.MAX_VALUE,
                Double.MIN_VALUE,
                Float.MAX_VALUE,
                Float.MIN_VALUE,
                Long.MAX_VALUE,
                Long.MIN_VALUE,
        };

        doubleFieldValues = new FieldValues<Double>() {
            @Override
            public boolean isEmpty() {
                return false;
            }

            @Override
            public int size() {
                return rawDoubleValues.length;
            }

            @Override
            public List<Double> getValues() {
                return DoubleStream.of(rawDoubleValues).boxed().collect(Collectors.toList());
            }

            @Override
            public Double getNonPrimitiveValue() {
                return rawDoubleValues[0];
            }

            @Override
            public long getLongValue() {
                return (long)rawDoubleValues[0];
            }

            @Override
            public double getDoubleValue() {
                return rawDoubleValues[0];
            }
        };

        doubleField = new DoubleField("double_field", doubleFieldValues);
    }

    protected BigInteger[] rawBigIntegerValues;
    protected FieldValues<BigInteger> bigIntegerFieldValues;
    protected Field<BigInteger> bigIntegerField;

    @Before
    public void setupBigIntegerField() {
        rawBigIntegerValues = new BigInteger[] {
                BigInteger.valueOf(123),
                BigDecimal.valueOf(Double.MAX_VALUE).toBigInteger(),
                BigDecimal.valueOf(Double.MIN_VALUE).toBigInteger(),
                BigDecimal.valueOf(Float.MAX_VALUE).toBigInteger(),
                BigDecimal.valueOf(Float.MIN_VALUE).toBigInteger(),
                BigInteger.valueOf(Long.MAX_VALUE),
                BigInteger.valueOf(Long.MIN_VALUE),
                BigInteger.ZERO,
                BigInteger.ONE,
                BigInteger.TWO,
                BigInteger.valueOf(-1)
        };

        bigIntegerFieldValues = new FieldValues<BigInteger>() {
            @Override
            public boolean isEmpty() {
                return false;
            }

            @Override
            public int size() {
                return rawBigIntegerValues.length;
            }

            @Override
            public List<BigInteger> getValues() {
                return Stream.of(rawBigIntegerValues).collect(Collectors.toList());
            }

            @Override
            public BigInteger getNonPrimitiveValue() {
                return rawBigIntegerValues[0];
            }

            @Override
            public long getLongValue() {
                return rawBigIntegerValues[0].longValue();
            }

            @Override
            public double getDoubleValue() {
                return rawBigIntegerValues[0].doubleValue();
            }
        };

        bigIntegerField = new BigIntegerField("big_integer_field", bigIntegerFieldValues);
    }

    protected String[] rawStringValuesAsLongs;
    protected FieldValues<String> stringFieldValuesAsLongs;
    protected Field<String> stringFieldAsLongs;

    @Before
    public void setupStringFieldAsLongs() {
        rawStringValuesAsLongs = new String[] {
                "72",
                "0",
                "-1",
                "1",
                ((Integer)Integer.MAX_VALUE).toString(),
                ((Integer)Integer.MIN_VALUE).toString(),
                ((Long)Long.MAX_VALUE).toString(),
                ((Long)Long.MIN_VALUE).toString()
        };

        stringFieldValuesAsLongs = new FieldValues<String>() {
            @Override
            public boolean isEmpty() {
                return false;
            }

            @Override
            public int size() {
                return rawStringValuesAsLongs.length;
            }

            @Override
            public List<String> getValues() {
                return Stream.of(rawStringValuesAsLongs).collect(Collectors.toList());
            }

            @Override
            public String getNonPrimitiveValue() {
                return rawStringValuesAsLongs[0];
            }

            @Override
            public long getLongValue() {
                return Long.parseLong(rawStringValuesAsLongs[0]);
            }

            @Override
            public double getDoubleValue() {
                return Double.parseDouble(rawStringValuesAsLongs[0]);
            }
        };

        stringFieldAsLongs = new StringField("string_field_as_longs", stringFieldValuesAsLongs);
    }

    protected String[] rawStringValuesAsDoubles;
    protected FieldValues<String> stringFieldValuesAsDoubles;
    protected Field<String> stringFieldAsDoubles;

    @Before
    public void setupStringFieldAsDoubles() {
        rawStringValuesAsDoubles = new String[] {
                "72",
                "0",
                "-1",
                "1",
                ((Long)Long.MAX_VALUE).toString(),
                ((Long)Long.MIN_VALUE).toString(),
                ((Double)Double.MAX_VALUE).toString(),
                ((Double)Double.MIN_VALUE).toString()
        };

        stringFieldValuesAsDoubles = new FieldValues<String>() {
            @Override
            public boolean isEmpty() {
                return false;
            }

            @Override
            public int size() {
                return rawStringValuesAsDoubles.length;
            }

            @Override
            public List<String> getValues() {
                return Stream.of(rawStringValuesAsDoubles).collect(Collectors.toList());
            }

            @Override
            public String getNonPrimitiveValue() {
                return rawStringValuesAsDoubles[0];
            }

            @Override
            public long getLongValue() {
                return Long.parseLong(rawStringValuesAsDoubles[0]);
            }

            @Override
            public double getDoubleValue() {
                return Double.parseDouble(rawStringValuesAsDoubles[0]);
            }
        };

        stringFieldAsDoubles = new StringField("string_field_as_doubles", stringFieldValuesAsDoubles);
    }

    long[] rawLongMillisValues;
    List<ZonedDateTime> rawDateMillisValues;
    protected FieldValues<ZonedDateTime> dateMillisFieldValues;
    protected Field<ZonedDateTime> dateMillisField;

    @Before
    public void setupDateMillisField() {
        rawLongMillisValues = new long[] {
                1629830752000L,
                0L,
                2040057952000L,
                -6106212564000L
        };

        rawDateMillisValues = List.of(
                ZonedDateTime.ofInstant(Instant.ofEpochMilli(rawLongMillisValues[0]), ZoneOffset.ofHours(-7)),
                ZonedDateTime.ofInstant(Instant.ofEpochMilli(rawLongMillisValues[1]), ZoneOffset.ofHours(-6)),
                ZonedDateTime.ofInstant(Instant.ofEpochMilli(rawLongMillisValues[2]), ZoneOffset.ofHours(0)),
                ZonedDateTime.ofInstant(Instant.ofEpochMilli(rawLongMillisValues[3]), ZoneOffset.ofHours(-5))
        );

        dateMillisFieldValues = new FieldValues<ZonedDateTime>() {
            @Override
            public boolean isEmpty() {
                return false;
            }

            @Override
            public int size() {
                return rawDateMillisValues.size();
            }

            @Override
            public List<ZonedDateTime> getValues() {
                return Collections.unmodifiableList(rawDateMillisValues);
            }

            @Override
            public ZonedDateTime getNonPrimitiveValue() {
                return rawDateMillisValues.get(0);
            }

            @Override
            public long getLongValue() {
                return DateMillisField.toLong(rawDateMillisValues.get(0));
            }

            @Override
            public double getDoubleValue() {
                return getLongValue();
            }
        };

        dateMillisField = new DateMillisField("millis_date_field", dateMillisFieldValues);
    }

    long[] rawLongNanosValues;
    List<ZonedDateTime> rawDateNanosValues;
    protected FieldValues<ZonedDateTime> dateNanosFieldValues;
    protected Field<ZonedDateTime> dateNanosField;

    @Before
    public void setupDateNanosField() {
        rawLongNanosValues = new long[] {
                1629830752000L,
                0L,
                2040057952000L,
                -6106212564000L
        };

        rawDateNanosValues = List.of(
                ZonedDateTime.ofInstant(Instant.EPOCH.plusNanos(rawLongNanosValues[0]), ZoneOffset.ofHours(-7)),
                ZonedDateTime.ofInstant(Instant.EPOCH.plusNanos(rawLongNanosValues[1]), ZoneOffset.ofHours(-6)),
                ZonedDateTime.ofInstant(Instant.EPOCH.plusNanos(rawLongNanosValues[2]), ZoneOffset.ofHours(0)),
                ZonedDateTime.ofInstant(Instant.EPOCH.plusNanos(rawLongNanosValues[3]), ZoneOffset.ofHours(-5))
        );

        dateNanosFieldValues = new FieldValues<ZonedDateTime>() {
            @Override
            public boolean isEmpty() {
                return false;
            }

            @Override
            public int size() {
                return rawDateNanosValues.size();
            }

            @Override
            public List<ZonedDateTime> getValues() {
                return Collections.unmodifiableList(rawDateNanosValues);
            }

            @Override
            public ZonedDateTime getNonPrimitiveValue() {
                return rawDateNanosValues.get(0);
            }

            @Override
            public long getLongValue() {
                return DateNanosField.toLong(rawDateNanosValues.get(0));
            }

            @Override
            public double getDoubleValue() {
                return getLongValue();
            }
        };

        dateNanosField = new DateNanosField("nanos_date_field", dateNanosFieldValues);
    }

    List<GeoPoint> rawGeoPointValues;
    protected FieldValues<GeoPoint> getPointFieldValues;
    protected Field<GeoPoint> getPointField;

    @Before
    public void setupGeoPointField() {
        rawGeoPointValues = List.of(
                new GeoPoint(0.0, 0.0)
        );

        getPointFieldValues = new FieldValues<GeoPoint>() {
            @Override
            public boolean isEmpty() {
                return false;
            }

            @Override
            public int size() {
                return rawGeoPointValues.size();
            }

            @Override
            public List<GeoPoint> getValues() {
                return Collections.unmodifiableList(rawGeoPointValues);
            }

            @Override
            public GeoPoint getNonPrimitiveValue() {
                return rawGeoPointValues.get(0);
            }

            @Override
            public long getLongValue() {
                throw new UnsupportedOperationException();
            }

            @Override
            public double getDoubleValue() {
                throw new UnsupportedOperationException();
            }
        };

        getPointField = new GeoPointField("geo_point_field", getPointFieldValues);
    }
}
