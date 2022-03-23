/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Processor for binary mathematical operations that have a second optional parameter.
 */
public class BinaryOptionalMathProcessor implements Processor {

    public enum BinaryOptionalMathOperation implements BiFunction<Number, Number, Number> {

        ROUND((l, r) -> {
            long rLong = r.longValue();
            if (l instanceof Long || l instanceof Integer || l instanceof Short || l instanceof Byte) {
                long lLong = l.longValue();
                if (lLong == 0L || rLong >= 0) {
                    return l;
                }

                long digitsToRound = -rLong;
                int digits = (int) (Math.log10(Math.abs(l.doubleValue())) + 1);
                if (digits <= digitsToRound) {
                    return convertToIntegerType(0L, l.getClass());
                }

                long tenAtScale = (long) tenPower(digitsToRound);
                long middleResult = lLong / tenAtScale;
                long remainder = lLong % tenAtScale;
                if (remainder >= 5 * (long) tenPower(digitsToRound - 1)) {
                    middleResult++;
                } else if (remainder <= -5 * (long) tenPower(digitsToRound - 1)) {
                    middleResult--;
                }

                long result = middleResult * tenAtScale;
                if (Long.signum(result) == Long.signum(lLong)) {
                    return convertToIntegerType(result, l.getClass());
                } else {
                    throw new ArithmeticException("long overflow");
                }
            }
            if (Double.isNaN(l.doubleValue())) {
                return l instanceof Float ? 0.0f : 0.0d;
            }

            double tenAtScale = tenPower(rLong);
            if (tenAtScale == 0.0) {
                return l instanceof Float ? 0.0f : 0.0d;
            }

            double middleResult = l.doubleValue() * tenAtScale;
            int sign = middleResult > 0 ? 1 : -1;

            if (Double.POSITIVE_INFINITY == middleResult || Double.NEGATIVE_INFINITY == middleResult) {
                return l;
            }
            if (Long.MIN_VALUE + 1 < middleResult && middleResult < Long.MAX_VALUE) {
                // the result can still be rounded using Math.round(), that is limited to long values
                Double result = Math.round(Math.abs(middleResult)) / tenAtScale * sign;
                return l instanceof Float ? result.floatValue() : result;
            }

            // otherwise fall back to BigDecimal, that is ~40x slower, but works fine
            MathContext prec = MathContext.DECIMAL128;
            Double result = new BigDecimal(Math.abs(middleResult), prec).round(new MathContext(0))
                .divide(new BigDecimal(tenAtScale), prec)
                .doubleValue() * sign;
            return l instanceof Float ? result.floatValue() : result;
        }),
        TRUNCATE((l, r) -> {
            long rLong = r.longValue();
            if (l instanceof Long || l instanceof Integer || l instanceof Short || l instanceof Byte) {
                long lLong = l.longValue();
                if (lLong == 0L || rLong >= 0) {
                    return l;
                }

                long digitsToTruncate = -rLong;
                int digits = (int) (Math.log10(Math.abs(l.doubleValue())) + 1);
                if (digits <= digitsToTruncate) {
                    return convertToIntegerType(0L, l.getClass());
                }

                long tenAtScale = (long) tenPower(digitsToTruncate);
                return convertToIntegerType((lLong / tenAtScale) * tenAtScale, l.getClass());
            }
            double tenAtScale = Math.pow(10., rLong);
            double g = l.doubleValue() * tenAtScale;
            Double result = (((l.doubleValue() < 0) ? Math.ceil(g) : Math.floor(g)) / tenAtScale);
            return l instanceof Float ? result.floatValue() : result;
        });

        private final BiFunction<Number, Number, Number> process;

        BinaryOptionalMathOperation(BiFunction<Number, Number, Number> process) {
            this.process = process;
        }

        @Override
        public final Number apply(Number left, Number right) {
            if (left == null) {
                return null;
            }
            if ((left instanceof Number) == false) {
                throw new SqlIllegalArgumentException("A number is required; received [{}]", left);
            }

            if (right != null) {
                if ((right instanceof Number) == false) {
                    throw new SqlIllegalArgumentException("A number is required; received [{}]", right);
                }
                if (right instanceof Float || right instanceof Double) {
                    throw new SqlIllegalArgumentException("An integer number is required; received [{}] as second parameter", right);
                }
            } else {
                right = 0;
            }

            return process.apply(left, right);
        }
    }

    // optimise very common cases for round and truncate
    private static double tenPower(long n) {
        if (n == 0L) {
            return 1d;
        } else if (n == 1L) {
            return 10d;
        } else if (n == 2L) {
            return 100d;
        } else if (n == 3L) {
            return 1000d;
        } else if (n == 4L) {
            return 10000d;
        } else if (n == 5L) {
            return 100000d;
        }
        return Math.pow(10, n);
    }

    /**
     * does not take number precision and overflow into consideration!
     * Use only in cases when these aspects are guaranteed by previous logic (eg. ROUND, TRUNCATE)
     * @param number the number to convert
     * @param type the destination type
     * @return the same number converted to the right type
     * @throws ArithmeticException in case of integer overflow.
     * See {@link org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Arithmetics}
     */
    private static Number convertToIntegerType(Long number, Class<? extends Number> type) throws ArithmeticException {
        if (type == Integer.class) {
            if (number > Integer.MAX_VALUE || number < Integer.MIN_VALUE) {
                throw new ArithmeticException("integer overflow");
            }
            return number.intValue();
        } else if (type == Short.class) {
            return number.shortValue();
        } else if (type == Byte.class) {
            return number.byteValue();
        }
        return number;
    }

    private final Processor left, right;
    private final BinaryOptionalMathOperation operation;
    public static final String NAME = "mob";

    public BinaryOptionalMathProcessor(Processor left, Processor right, BinaryOptionalMathOperation operation) {
        this.left = left;
        this.right = right;
        this.operation = operation;
    }

    public BinaryOptionalMathProcessor(StreamInput in) throws IOException {
        left = in.readNamedWriteable(Processor.class);
        right = in.readOptionalNamedWriteable(Processor.class);
        operation = in.readEnum(BinaryOptionalMathOperation.class);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(left);
        out.writeOptionalNamedWriteable(right);
        out.writeEnum(operation);
    }

    @Override
    public Object process(Object input) {
        return doProcess(left().process(input), right() == null ? null : right().process(input));
    }

    public Number doProcess(Object left, Object right) {
        if (left == null) {
            return null;
        }
        if ((left instanceof Number) == false) {
            throw new SqlIllegalArgumentException("A number is required; received [{}]", left);
        }

        if (right != null) {
            if ((right instanceof Number) == false) {
                throw new SqlIllegalArgumentException("A number is required; received [{}]", right);
            }
            if (right instanceof Float || right instanceof Double) {
                throw new SqlIllegalArgumentException("An integer number is required; received [{}] as second parameter", right);
            }
        } else {
            right = 0;
        }

        return operation().apply((Number) left, (Number) right);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        BinaryOptionalMathProcessor other = (BinaryOptionalMathProcessor) obj;
        return Objects.equals(left(), other.left())
            && Objects.equals(right(), other.right())
            && Objects.equals(operation(), other.operation());
    }

    @Override
    public int hashCode() {
        return Objects.hash(left(), right(), operation());
    }

    public Processor left() {
        return left;
    }

    public Processor right() {
        return right;
    }

    public BinaryOptionalMathOperation operation() {
        return operation;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
