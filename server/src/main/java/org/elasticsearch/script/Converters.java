/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.script.Field.BigIntegerField;
import static org.elasticsearch.script.Field.BooleanField;
import static org.elasticsearch.script.Field.DoubleField;
import static org.elasticsearch.script.Field.DateMillisField;
import static org.elasticsearch.script.Field.DateNanosField;
import static org.elasticsearch.script.Field.LongField;
import static org.elasticsearch.script.Field.StringField;

/**
 * {@link Converters} for scripting fields.  These constants are exposed as static fields on {@link Field} to
 * allow a user to convert via {@link Field#as(Converter)}.
 */
public class Converters {
    /**
     * Convert to a {@link BigIntegerField} from Long, Double or String Fields.
     * Longs and Doubles are wrapped as BigIntegers.
     * Strings are parsed as either Longs or Doubles and wrapped in a BigInteger.
     */
    static final Converter<BigInteger, BigIntegerField> BIGINTEGER;

    /**
     * Convert to a {@link LongField} from Double, String, DateMillis, DateNanos, BigInteger or Boolean Fields.
     * Double is cast to a Long.
     * String is parsed as a Long.
     * DateMillis is milliseconds since epoch.
     * DateNanos is nanoseconds since epoch.
     * {@link BigInteger#longValue()} is used for the BigInteger conversion.
     * Boolean is {@code 1L} if {@code true}, {@code 0L} if {@code false}.
     */
    static final Converter<Long, LongField> LONG;

    static {
        BIGINTEGER = new Converter<BigInteger, BigIntegerField>() {
            @Override
            public BigIntegerField convert(Field<?> sourceField) {
                if (sourceField instanceof LongField) {
                    return convertLongToBigIntegerField((LongField) sourceField);
                }

                if (sourceField instanceof DoubleField) {
                    return convertDoubleToBigIntegerField((DoubleField) sourceField);
                }

                if (sourceField instanceof StringField) {
                    return convertStringToBigIntegerField((StringField) sourceField);
                }

                if (sourceField instanceof DateMillisField) {
                    return convertLongToBigIntegerField(convertDateMillisToLongField((DateMillisField) sourceField));
                }

                if (sourceField instanceof DateNanosField) {
                    return convertLongToBigIntegerField(convertDateNanosToLongField((DateNanosField) sourceField));
                }

                if (sourceField instanceof BooleanField) {
                    return convertLongToBigIntegerField(convertBooleanToLongField((BooleanField) sourceField));
                }

                throw new InvalidConversion(sourceField.getClass(), getFieldClass());
            }

            @Override
            public Class<BigIntegerField> getFieldClass() {
                return BigIntegerField.class;
            }

            @Override
            public Class<BigInteger> getTargetClass() {
                return BigInteger.class;
            }
        };

        LONG = new Converter<Long, LongField>() {
            @Override
            public LongField convert(Field<?> sourceField) {
                if (sourceField instanceof DoubleField) {
                    return convertDoubleToLongField((DoubleField) sourceField);
                }

                if (sourceField instanceof StringField) {
                    return convertStringToLongField((StringField) sourceField);
                }

                if (sourceField instanceof DateMillisField) {
                    return convertDateMillisToLongField((DateMillisField) sourceField);
                }

                if (sourceField instanceof DateNanosField) {
                    return convertDateNanosToLongField((DateNanosField) sourceField);
                }

                if (sourceField instanceof BigIntegerField) {
                    return convertBigIntegerToLongField((BigIntegerField) sourceField);
                }

                if (sourceField instanceof BooleanField) {
                    return convertBooleanToLongField((BooleanField) sourceField);
                }

                throw new InvalidConversion(sourceField.getClass(), getFieldClass());
            }

            @Override
            public Class<LongField> getFieldClass() {
                return LongField.class;
            }

            @Override
            public Class<Long> getTargetClass() {
                return Long.class;
            }
        };
    }

    // No instances, please
    private Converters() {}

    static BigIntegerField convertLongToBigIntegerField(LongField sourceField) {
        FieldValues<Long> fv = sourceField.getFieldValues();
        return new BigIntegerField(sourceField.getName(), new DelegatingFieldValues<BigInteger, Long>(fv) {
            @Override
            public List<BigInteger> getValues() {
                return values.getValues().stream().map(BigInteger::valueOf).collect(Collectors.toList());
            }

            @Override
            public BigInteger getNonPrimitiveValue() {
                return BigInteger.valueOf(values.getLongValue());
            }
        });
    }

    static BigIntegerField convertDoubleToBigIntegerField(DoubleField sourceField) {
        FieldValues<Double> fv = sourceField.getFieldValues();
        return new BigIntegerField(sourceField.getName(), new DelegatingFieldValues<BigInteger, Double>(fv) {
            @Override
            public List<BigInteger> getValues() {
                return values.getValues().stream().map(Converters::convertDoubleToBigInteger).collect(Collectors.toList());
            }

            @Override
            public BigInteger getNonPrimitiveValue() {
                return convertDoubleToBigInteger(values.getDoubleValue());
            }
        });
    }

    static BigIntegerField convertStringToBigIntegerField(StringField sourceField) {
        FieldValues<String> fv = sourceField.getFieldValues();
        return new BigIntegerField(sourceField.getName(), new DelegatingFieldValues<BigInteger, String>(fv) {
            @Override
            public List<BigInteger> getValues() {
                // This may throw NumberFormatException, should we catch and truncate the List? (#76951)
                return values.getValues().stream().map(Converters::convertStringToBigInteger).collect(Collectors.toList());
            }

            @Override
            public BigInteger getNonPrimitiveValue() {
                return convertStringToBigInteger(values.getNonPrimitiveValue());
            }

            @Override
            public long getLongValue() {
                return getNonPrimitiveValue().longValue();
            }

            @Override
            public double getDoubleValue() {
                return getNonPrimitiveValue().doubleValue();
            }
        });
    }

    static LongField convertBigIntegerToLongField(BigIntegerField sourceField) {
        FieldValues<BigInteger> fv = sourceField.getFieldValues();
        return new LongField(sourceField.getName(), new DelegatingFieldValues<Long, BigInteger>(fv) {
            @Override
            public List<Long> getValues() {
                return values.getValues().stream().map(BigInteger::longValue).collect(Collectors.toList());
            }

            @Override
            public Long getNonPrimitiveValue() {
                return convertBigIntegerToLong(values.getNonPrimitiveValue());
            }

            @Override
            public long getLongValue() {
                return convertBigIntegerToLong(values.getNonPrimitiveValue());
            }

            @Override
            public double getDoubleValue() {
                return convertBigIntegerToLong(values.getNonPrimitiveValue());
            }
        });
    }

    static LongField convertBooleanToLongField(BooleanField sourceField) {
        FieldValues<Boolean> fv = sourceField.getFieldValues();
        return new LongField(sourceField.getName(), new DelegatingFieldValues<Long, Boolean>(fv) {
            @Override
            public List<Long> getValues() {
                return values.getValues().stream().map(bool -> bool ? 1L : 0L).collect(Collectors.toList());
            }

            @Override
            public Long getNonPrimitiveValue() {
                return convertBooleanToLong(values.getNonPrimitiveValue());
            }

            @Override
            public long getLongValue() {
                return convertBooleanToLong(values.getNonPrimitiveValue());
            }

            @Override
            public double getDoubleValue() {
                return convertBooleanToLong(values.getNonPrimitiveValue());
            }
        });
    }

    static LongField convertDateMillisToLongField(DateMillisField sourceField) {
        FieldValues<JodaCompatibleZonedDateTime> fv = sourceField.getFieldValues();
        return new LongField(sourceField.getName(), new DelegatingFieldValues<Long, JodaCompatibleZonedDateTime>(fv) {
            @Override
            public List<Long> getValues() {
                return values.getValues().stream().map(dt -> dt.toInstant().toEpochMilli()).collect(Collectors.toList());
            }

            @Override
            public Long getNonPrimitiveValue() {
                return convertDateMillisToLong(values.getNonPrimitiveValue());
            }

            @Override
            public long getLongValue() {
                return convertDateMillisToLong(values.getNonPrimitiveValue());
            }

            @Override
            public double getDoubleValue() {
                return convertDateMillisToLong(values.getNonPrimitiveValue());
            }
        });
    }

    static LongField convertDateNanosToLongField(DateNanosField sourceField) {
        FieldValues<JodaCompatibleZonedDateTime> fv = sourceField.getFieldValues();
        return new LongField(sourceField.getName(), new DelegatingFieldValues<Long, JodaCompatibleZonedDateTime>(fv) {
            protected long nanoLong(JodaCompatibleZonedDateTime dt) {
                return ChronoUnit.NANOS.between(Instant.EPOCH, dt.toInstant());
            }

            @Override
            public List<Long> getValues() {
                return values.getValues().stream().map(this::nanoLong).collect(Collectors.toList());
            }

            @Override
            public Long getNonPrimitiveValue() {
                return convertDateNanosToLong(values.getNonPrimitiveValue());
            }

            @Override
            public long getLongValue() {
                return convertDateNanosToLong(values.getNonPrimitiveValue());
            }

            @Override
            public double getDoubleValue() {
                return convertDateNanosToLong(values.getNonPrimitiveValue());
            }
        });
    }

    static LongField convertDoubleToLongField(DoubleField sourceField) {
        FieldValues<Double> fv = sourceField.getFieldValues();
        return new LongField(sourceField.getName(), new DelegatingFieldValues<Long, Double>(fv) {
            @Override
            public List<Long> getValues() {
                return values.getValues().stream().map(Double::longValue).collect(Collectors.toList());
            }

            @Override
            public Long getNonPrimitiveValue() {
                return values.getLongValue();
            }
        });
    }

    static LongField convertStringToLongField(StringField sourceField) {
        FieldValues<String> fv = sourceField.getFieldValues();
        return new LongField(sourceField.getName(), new DelegatingFieldValues<Long, String>(fv) {
            @Override
            public List<Long> getValues() {
                return values.getValues().stream().map(Long::parseLong).collect(Collectors.toList());
            }

            @Override
            public Long getNonPrimitiveValue() {
                return convertStringToLong(values.getNonPrimitiveValue());
            }

            @Override
            public long getLongValue() {
                return convertStringToLong(values.getNonPrimitiveValue());
            }

            @Override
            public double getDoubleValue() {
                // conversion is to LongField, doesn't make sense to parse a Double out of the String here.
                return convertStringToLong(values.getNonPrimitiveValue());
            }
        });
    }

    public static long convertBigIntegerToLong(BigInteger bigInteger) {
        return bigInteger.longValue();
    }

    public static double convertBigIntegerToDouble(BigInteger bigInteger) {
        return bigInteger.doubleValue();
    }

    public static long convertBooleanToLong(boolean bool) {
        return bool ? 1L : 0L;
    }

    public static double convertBooleanToDouble(boolean bool) {
        return bool ? 1.0d : 0.0d;
    }

    public static long convertDateMillisToLong(JodaCompatibleZonedDateTime dt) {
        return dt.toInstant().toEpochMilli();
    }

    public static long convertDateNanosToLong(JodaCompatibleZonedDateTime dt) {
        return ChronoUnit.NANOS.between(Instant.EPOCH, dt.toInstant());
    }

    public static BigInteger convertDoubleToBigInteger(double dbl) {
        return BigDecimal.valueOf(dbl).toBigInteger();
    }

    // String
    public static BigInteger convertStringToBigInteger(String str) {
        try {
            return new BigInteger(str);
        } catch (NumberFormatException e) {
            return new BigDecimal(str).toBigInteger();
        }
    }

    public static double convertStringToDouble(String str) {
        return Double.parseDouble(str);
    }

    public static long convertStringToLong(String str) {
        return Long.parseLong(str);
    }


    /**
     * Helper for creating {@link Converter} classes which delegates all un-overridden methods to the underlying
     * {@link FieldValues}.
     */
    public abstract static class DelegatingFieldValues<T, D> implements FieldValues<T> {
        protected FieldValues<D> values;

        public DelegatingFieldValues(FieldValues<D> values) {
            this.values = values;
        }

        @Override
        public boolean isEmpty() {
            return values.isEmpty();
        }

        @Override
        public int size() {
            return values.size();
        }

        @Override
        public long getLongValue() {
            return values.getLongValue();
        }

        @Override
        public double getDoubleValue() {
            return values.getDoubleValue();
        }
    }
}
