/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.compute.ann.MvEvaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isRepresentable;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;

/**
 * Reduce a multivalued field to a single valued field containing the average value.
 */
public class MvMedian extends AbstractMultivalueFunction {
    public MvMedian(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected TypeResolution resolveFieldType() {
        return isType(field(), t -> t.isNumeric() && isRepresentable(t), sourceText(), null, "numeric");
    }

    @Override
    protected Supplier<EvalOperator.ExpressionEvaluator> evaluator(Supplier<EvalOperator.ExpressionEvaluator> fieldEval) {
        return switch (LocalExecutionPlanner.toElementType(field().dataType())) {
            case DOUBLE -> () -> new MvMedianDoubleEvaluator(fieldEval.get());
            case INT -> () -> new MvMedianIntEvaluator(fieldEval.get());
            case LONG -> () -> new MvMedianLongEvaluator(fieldEval.get());
            default -> throw new UnsupportedOperationException("unsupported type [" + field().dataType() + "]");
        };
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvMedian(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvMedian::new, field());
    }

    static class Doubles {
        public double[] values = new double[2];
        public int count;
    }

    @MvEvaluator(extraName = "Double", finish = "finish")
    static void process(Doubles doubles, double v) {
        if (doubles.values.length < doubles.count + 1) {
            doubles.values = ArrayUtil.grow(doubles.values, doubles.count + 1);
        }
        doubles.values[doubles.count++] = v;
    }

    static double finish(Doubles doubles) {
        // TODO quickselect
        Arrays.sort(doubles.values, 0, doubles.count);
        int middle = doubles.count / 2;
        double median = doubles.count % 2 == 1 ? doubles.values[middle] : (doubles.values[middle - 1] + doubles.values[middle]) / 2;
        doubles.count = 0;
        return median;
    }

    static class Longs {
        public long[] values = new long[2];
        public int count;
    }

    @MvEvaluator(extraName = "Long", finish = "finish")
    static void process(Longs longs, long v) {
        if (longs.values.length < longs.count + 1) {
            longs.values = ArrayUtil.grow(longs.values, longs.count + 1);
        }
        longs.values[longs.count++] = v;
    }

    static long finish(Longs longs) {
        // TODO quickselect
        Arrays.sort(longs.values, 0, longs.count);
        int middle = longs.count / 2;
        long median = longs.count % 2 == 1 ? longs.values[middle] : (longs.values[middle - 1] + longs.values[middle]) >>> 1;
        longs.count = 0;
        return median;
    }

    static class Ints {
        public int[] values = new int[2];
        public int count;
    }

    @MvEvaluator(extraName = "Int", finish = "finish")
    static void process(Ints ints, int v) {
        if (ints.values.length < ints.count + 1) {
            ints.values = ArrayUtil.grow(ints.values, ints.count + 1);
        }
        ints.values[ints.count++] = v;
    }

    static int finish(Ints ints) {
        // TODO quickselect
        Arrays.sort(ints.values, 0, ints.count);
        int middle = ints.count / 2;
        int median = ints.count % 2 == 1 ? ints.values[middle] : (ints.values[middle - 1] + ints.values[middle]) >>> 1;
        ints.count = 0;
        return median;
    }
}
