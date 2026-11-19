/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A {@link NestedAny} lowered to one boolean per parent. The predicate body is an expression child so optimizer
 * rewrites, including surrogate substitution, see it. {@link #references()} stays empty: those fields are loaded from
 * the child documents, and the parent {@code _doc} stays on the page through
 * {@link org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LoadNestedFromIndex} rather than as a column the
 * query names.
 */
public final class NestedPredicate extends EsqlScalarFunction {
    public static final TransportVersion VERSION = TransportVersion.fromName("esql_nested_any");
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "NestedPredicate",
        NestedPredicate::new
    );

    private final String path;
    private final Expression body;
    /** Child fields of {@link #body}, in the order {@link NestedChildren} loads them. */
    private final List<Attribute> inputs;
    private final List<String> relativePaths;

    public NestedPredicate(Source source, String path, Expression body, List<Attribute> inputs, List<String> relativePaths) {
        super(source, List.of(body));
        this.path = path;
        this.body = body;
        this.inputs = List.copyOf(inputs);
        this.relativePaths = List.copyOf(relativePaths);
    }

    private NestedPredicate(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readString(),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readStringCollectionAsList()
        );
    }

    public static void checkVersion(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(VERSION) == false) {
            throw new IOException("NESTED_ANY requires nested predicate transport support");
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        checkVersion(out);
        source().writeTo(out);
        out.writeString(path);
        out.writeNamedWriteable(body);
        out.writeNamedWriteableCollection(inputs);
        out.writeStringCollection(relativePaths);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public String path() {
        return path;
    }

    public Expression body() {
        return body;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    public boolean foldable() {
        return false;
    }

    @Override
    protected TypeResolution resolveType() {
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    protected NodeInfo<NestedPredicate> info() {
        return NodeInfo.create(this, NestedPredicate::new, path, body, inputs, relativePaths);
    }

    /**
     * Child fields are loaded from the nested documents. Reporting them would make field extraction look for those
     * columns on the parent page.
     */
    @Override
    public AttributeSet references() {
        return AttributeSet.EMPTY;
    }

    @Override
    public NestedPredicate replaceChildren(List<Expression> newChildren) {
        return new NestedPredicate(source(), path, newChildren.getFirst(), inputs, relativePaths);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj)
            && obj instanceof NestedPredicate other
            && path.equals(other.path)
            && body.equals(other.body)
            && inputs.equals(other.inputs)
            && relativePaths.equals(other.relativePaths);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), children(), path, body, inputs, relativePaths);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(EvaluatorMapper.ToEvaluator toEvaluator) {
        Layout layout = new Layout.Builder().append(inputs).build();
        FoldContext foldCtx = toEvaluator.foldCtx();
        ExpressionEvaluator.Factory predicate = EvalMapper.toEvaluator(foldCtx, body, layout, toEvaluator.shardContexts(), null);
        List<DataType> types = inputs.stream().map(Attribute::dataType).toList();
        // The expand command threads the query's planner settings. A predicate uses the defaults: the budgets change how
        // large a child page may be, not the values a small nested object reads.
        NestedChildren.Factory children = NestedChildren.factory(
            toEvaluator.shardContexts(),
            path,
            relativePaths,
            types,
            Collections.nCopies(types.size(), (ValuesSourceReaderOperator.ConverterFactory) null),
            PlannerSettings.DEFAULTS
        );
        int[] fieldChannels = new int[inputs.size()];
        for (int i = 0; i < fieldChannels.length; i++) {
            fieldChannels[i] = i + 1;
        }
        return new ExpressionEvaluator.Factory() {
            @Override
            public ExpressionEvaluator get(DriverContext context) {
                NestedChildren read = children.get(context);
                ExpressionEvaluator eval = predicate.get(context);
                return new Evaluator(context, read, eval, fieldChannels);
            }
        };
    }

    private static final class Evaluator implements ExpressionEvaluator {
        private final DriverContext context;
        private final NestedChildren children;
        private final ExpressionEvaluator predicate;
        private final int[] fieldChannels;

        private Evaluator(DriverContext context, NestedChildren children, ExpressionEvaluator predicate, int[] fieldChannels) {
            this.context = context;
            this.children = children;
            this.predicate = predicate;
            this.fieldChannels = fieldChannels;
        }

        @Override
        public Block eval(Page page) {
            DocVector docs = docs(page);
            boolean[] matched = new boolean[docs.getPositionCount()];
            children.read(docs, position -> true, loaded -> accept(loaded, matched));
            BooleanBlock.Builder builder = context.blockFactory().newBooleanBlockBuilder(matched.length);
            Block result = null;
            try {
                for (boolean bit : matched) {
                    builder.appendBoolean(bit);
                }
                result = builder.build();
                return result;
            } finally {
                if (result == null) {
                    builder.close();
                }
            }
        }

        private void accept(Page loaded, boolean[] matched) {
            IntVector parents = loaded.<IntBlock>getBlock(0).asVector();
            if (parents == null) {
                throw new IllegalStateException("NESTED_ANY expected one parent position per child");
            }
            if (fieldChannels.length == 0) {
                Block constant = predicate.eval(new Page(1));
                try {
                    boolean childMatched = Boolean.TRUE.equals(BlockUtils.toJavaObject(constant, 0));
                    for (int c = 0; c < parents.getPositionCount(); c++) {
                        matched[parents.getInt(c)] |= childMatched;
                    }
                } finally {
                    constant.close();
                }
                return;
            }
            Page fields = loaded.projectBlocks(fieldChannels);
            Block values = null;
            try {
                values = predicate.eval(fields);
                for (int c = 0; c < values.getPositionCount(); c++) {
                    if (values.getValueCount(c) > 1) {
                        throw new IllegalArgumentException("NESTED_ANY requires a scalar predicate result per child");
                    }
                    boolean childMatched = Boolean.TRUE.equals(BlockUtils.toJavaObject(values, c));
                    matched[parents.getInt(c)] |= childMatched;
                }
            } finally {
                if (values != null) {
                    values.close();
                }
                fields.releaseBlocks();
            }
        }

        private static DocVector docs(Page page) {
            for (int i = 0; i < page.getBlockCount(); i++) {
                if (page.getBlock(i) instanceof DocBlock doc) {
                    return doc.asVector();
                }
            }
            throw new IllegalStateException("NESTED_ANY needs the parent documents, which this page does not carry");
        }

        @Override
        public long baseRamBytesUsed() {
            return predicate.baseRamBytesUsed();
        }

        @Override
        public void close() {
            Releasables.close(predicate, children);
        }
    }
}
