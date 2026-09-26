/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.attributes;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.PackDimBlock;
import org.elasticsearch.compute.data.PackDimValue;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.expression.LoadFromPageEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Evaluates immutable dimension operations with ordinary scalar trees. Record-scoped evaluation is an explicit
 * planner decision; general updates retain row-wise semantics, including external column dependencies.
 */
final class PackDimEvaluator implements ExpressionEvaluator {
    static Factory factory(PackDimSupport expression, EvaluatorMapper.ToEvaluator mapper) {
        if (expression.operation() == PackDimSupport.Operation.UNSET) {
            BytesRef[] removed = expression.dimensions().stream().map(d -> new BytesRef(d.name())).sorted().toArray(BytesRef[]::new);
            return new Factory(
                expression,
                List.of(
                    mapper.apply(expression.children().getFirst()),
                    new PackDimValuesUnsetEvaluator.Factory(expression.source(), new LoadFromPageEvaluator.Factory(0), removed)
                )
            );
        }
        if (expression.operation() == PackDimSupport.Operation.SET_FROM_DIMENSIONS
            || expression.operation() == PackDimSupport.Operation.MAP_FROM_DIMENSIONS) {
            Expression input = expression.children().getFirst();
            return new Factory(expression, List.of(mapper.apply(input), compileRecord(expression.children().get(1), input, mapper)));
        }
        return new Factory(expression, expression.children().stream().map(mapper::apply).toList());
    }

    private static ExpressionEvaluator.Factory compileRecord(Expression expression, Expression input, EvaluatorMapper.ToEvaluator mapper) {
        if (expression.equals(input)) return new LoadFromPageEvaluator.Factory(0);
        if (expression instanceof Literal) return mapper.apply(expression);
        if (expression instanceof EvaluatorMapper evaluator) {
            return evaluator.toEvaluator(new EvaluatorMapper.ToEvaluator() {
                @Override
                public ExpressionEvaluator.Factory apply(Expression child) {
                    return compileRecord(child, input, mapper);
                }

                @Override
                public FoldContext foldCtx() {
                    return mapper.foldCtx();
                }
            });
        }
        throw new IllegalArgumentException("record-scoped expression references an external column: " + expression);
    }

    record Factory(PackDimSupport expression, List<ExpressionEvaluator.Factory> children) implements ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            ExpressionEvaluator[] evaluators = new ExpressionEvaluator[children.size()];
            boolean success = false;
            try {
                for (int i = 0; i < evaluators.length; i++)
                    evaluators[i] = children.get(i).get(context);
                var result = new PackDimEvaluator(context, expression, evaluators);
                success = true;
                return result;
            } finally {
                if (success == false) Releasables.closeExpectNoException(evaluators);
            }
        }
    }

    private final DriverContext context;
    private final PackDimSupport expression;
    private final ExpressionEvaluator[] children;
    private final BytesRef[] names;
    private final int[] channels;

    private PackDimEvaluator(DriverContext context, PackDimSupport expression, ExpressionEvaluator[] children) {
        this.context = context;
        this.expression = expression;
        this.children = children;
        BytesRef[] unsorted = expression.dimensions().stream().map(d -> new BytesRef(d.name())).toArray(BytesRef[]::new);
        channels = IntStream.range(0, unsorted.length).boxed().sorted(Comparator.comparing(i -> unsorted[i])).mapToInt(i -> i).toArray();
        names = Arrays.stream(channels).mapToObj(i -> unsorted[i]).toArray(BytesRef[]::new);
    }

    @Override
    public Block eval(Page page) {
        try {
            if (expression.operation() == PackDimSupport.Operation.PACK) return pack(page);
            try (Block raw = children[0].eval(page)) {
                if (raw.areAllValuesNull()) return context.blockFactory().newConstantNullBlock(page.getPositionCount());
                PackDimBlock input = (PackDimBlock) raw;
                if (expression.operation() == PackDimSupport.Operation.SET) {
                    try (Block values = children[1].eval(page)) {
                        return set(input, values);
                    }
                }
                if (expression.operation() == PackDimSupport.Operation.UNSET && names.length == 0) {
                    input.incRef();
                    return input;
                }
                try (var batch = new PackDimBatch(input)) {
                    PackDimBlock records = batch.records();
                    try (Block result = switch (expression.operation()) {
                        case GET -> project(records, names[0], expression.dimensions().getFirst());
                        case UNSET -> children[1].eval(new Page(records));
                        case MAP_FROM_DIMENSIONS -> children[1].eval(new Page(records));
                        case SET_FROM_DIMENSIONS -> {
                            // The page borrows the batch's records. Scalar evaluators retain normal lazy semantics.
                            try (Block values = children[1].eval(new Page(records))) {
                                yield set(records, values);
                            }
                        }
                        case PACK, SET -> throw new IllegalStateException("operation handled before record evaluation");
                    }) {
                        return batch.expand(result);
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private PackDimBlock pack(Page page) throws IOException {
        Block[] inputs = new Block[children.length];
        try (var output = context.blockFactory().newPackDimBlockBuilder(page.getPositionCount())) {
            for (int i = 0; i < inputs.length; i++)
                inputs[i] = children[i].eval(page);
            BytesRef[] values = new BytesRef[names.length];
            for (int p = 0; p < page.getPositionCount(); p++) {
                for (int i = 0; i < names.length; i++) {
                    values[i] = PackDimValues.encode(inputs[channels[i]], p);
                }
                output.append(names, values);
            }
            return output.build();
        } finally {
            Releasables.close(inputs);
        }
    }

    private Block project(PackDimBlock input, BytesRef name, Attribute dimension) throws IOException {
        try (
            var output = PackDimValues.elementType(dimension.dataType()).newBlockBuilder(input.getPositionCount(), context.blockFactory())
        ) {
            PackDimValue value = new PackDimValue();
            for (int p = 0; p < input.getPositionCount(); p++) {
                if (input.isNull(p)) output.appendNull();
                else PackDimValues.get(input.getPackDim(input.getFirstValueIndex(p), value), name, dimension.dataType(), output);
            }
            return output.build();
        }
    }

    private PackDimBlock set(PackDimBlock input, Block values) throws IOException {
        try (var output = context.blockFactory().newPackDimBlockBuilder(input.getPositionCount())) {
            PackDimValue value = new PackDimValue();
            for (int p = 0; p < input.getPositionCount(); p++) {
                if (input.isNull(p)) output.appendNull();
                else PackDimValues.set(
                    input.getPackDim(input.getFirstValueIndex(p), value),
                    names[0],
                    PackDimValues.encode(values, p),
                    output
                );
            }
            return output.build();
        }
    }

    @Override
    public void close() {
        Releasables.close(children);
    }

    @Override
    public long baseRamBytesUsed() {
        long bytes = RamUsageEstimator.shallowSizeOfInstance(PackDimEvaluator.class) + RamUsageEstimator.shallowSizeOf(children)
            + RamUsageEstimator.sizeOf(channels) + RamUsageEstimator.shallowSizeOf(names);
        for (BytesRef name : names)
            bytes += RamUsageEstimator.shallowSizeOf(name) + RamUsageEstimator.sizeOf(name.bytes);
        for (ExpressionEvaluator child : children)
            bytes += child.baseRamBytesUsed();
        return bytes;
    }

    @Override
    public String toString() {
        return expression.functionName()
            + "[dimensions="
            + expression.dimensions()
            + ", recordBatch="
            + (expression.operation() == PackDimSupport.Operation.SET_FROM_DIMENSIONS
                || expression.operation() == PackDimSupport.Operation.MAP_FROM_DIMENSIONS)
            + "]";
    }
}
