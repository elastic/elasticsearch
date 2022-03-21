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
            if (l instanceof Long || l instanceof Integer) {
                long lLong = l.longValue();
                if (lLong == 0L || rLong >= 0) {
                    return lLong;
                }

                long digitsToRound = -rLong;
                int digits = (int) (Math.log10(Math.abs(l.doubleValue())) + 1);
                if (digits <= digitsToRound) {
                    return 0L;
                }

                long divider = (long) Math.pow(10, digitsToRound);
                long middleResult = lLong / divider;
                long remainder = lLong % divider;
                if (remainder >= 5 * (long) Math.pow(10, digitsToRound - 1)) {
                    middleResult++;
                } else if (remainder <= -5 * (long) Math.pow(10, digitsToRound - 1)) {
                    middleResult--;
                }

                long result = middleResult * divider;
                if (Long.signum(result) == Long.signum(lLong)) {
                    return result;
                }// otherwise there was an overflow on long values, fall back to floating point implementation.

            }
            if (Double.isNaN(l.doubleValue())) {
                return 0.0;
            }

            double tenAtScale = Math.pow(10., rLong);
            if (tenAtScale == 0.0) {
                return 0.0;
            }

            double middleResult = l.doubleValue() * tenAtScale;
            int sign = middleResult > 0 ? 1 : -1;

            if (Double.POSITIVE_INFINITY == middleResult || Double.NEGATIVE_INFINITY == middleResult) {
                return l;
            }
            if (Long.MIN_VALUE + 1 < middleResult && middleResult < Long.MAX_VALUE) {
                // the result can still be rounded using Math.round(), that is limited to long values
                return Math.round(Math.abs(middleResult)) / tenAtScale * sign;
            }

            // otherwise fall back to BigDecimal, that is ~40x slower, but works fine
            MathContext prec = MathContext.DECIMAL128;
            return new BigDecimal(Math.abs(middleResult), prec).round(new MathContext(0))
                .divide(new BigDecimal(tenAtScale), prec)
                .doubleValue() * sign;
        }),
        TRUNCATE((l, r) -> {
            long rLong = r.longValue();
            if (l instanceof Long || l instanceof Integer) {
                long lLong = l.longValue();
                if (lLong == 0L || rLong >= 0) {
                    return lLong;
                }

                long digitsToTruncate = -rLong;
                int digits = (int) (Math.log10(Math.abs(l.doubleValue())) + 1);
                if (digits <= digitsToTruncate) {
                    return 0L;
                }

                long divider = (long) Math.pow(10, digitsToTruncate);
                return (lLong / divider) * divider;
            }
            double tenAtScale = Math.pow(10., rLong);
            double g = l.doubleValue() * tenAtScale;
            return (((l.doubleValue() < 0) ? Math.ceil(g) : Math.floor(g)) / tenAtScale);
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
