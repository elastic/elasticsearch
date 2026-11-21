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
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.lucene.query.LuceneQueryEvaluator.ShardConfig;
import org.elasticsearch.compute.lucene.query.LuceneQueryExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlConfigurationFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.QueryPlan;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link NestedAny} lowered to one boolean per parent. Its only child is the predicate body, whose child fields are loaded
 * from the child documents. The parent {@code _doc} stays on the page through
 * {@link org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LoadNestedFromIndex} rather than as a column the
 * query names.
 */
public final class NestedPredicate extends EsqlConfigurationFunction {
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
    /**
     * The nested query this predicate is exactly equivalent to on the shards of the node that plans it, or {@code null}.
     * With it the predicate is evaluated by Lucene, which stops at a parent's first matching child, instead of reading the
     * children. It is not serialized: each node decides from its own shards.
     */
    @Nullable
    private final QueryBuilder queryBuilder;

    public NestedPredicate(
        Source source,
        String path,
        Expression body,
        List<Attribute> inputs,
        List<String> relativePaths,
        @Nullable QueryBuilder queryBuilder,
        Configuration configuration
    ) {
        super(source, List.of(body), configuration);
        this.path = path;
        this.body = body;
        this.inputs = List.copyOf(inputs);
        this.relativePaths = List.copyOf(relativePaths);
        this.queryBuilder = queryBuilder;
    }

    private NestedPredicate(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readString(),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readStringCollectionAsList(),
            null,
            ((PlanStreamInput) in).configuration()
        );
    }

    /** Whether any expression of this node, not of its children, is a {@link NestedPredicate}. */
    public static boolean usedBy(QueryPlan<?> plan) {
        boolean[] found = { false };
        plan.forEachExpression(NestedPredicate.class, predicate -> found[0] = true);
        return found[0];
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

    public List<Attribute> inputs() {
        return inputs;
    }

    public List<String> relativePaths() {
        return relativePaths;
    }

    @Nullable
    public QueryBuilder queryBuilder() {
        return queryBuilder;
    }

    public NestedPredicate withQueryBuilder(QueryBuilder queryBuilder) {
        return new NestedPredicate(source(), path, body, inputs, relativePaths, queryBuilder, configuration());
    }

    /** The full names of the child fields this predicate reads. */
    public List<String> fieldNames() {
        return relativePaths.stream().map(relative -> path + "." + relative).toList();
    }

    /**
     * The predicate over the child fields themselves, for Lucene to match within each child document. {@code null} when
     * it reads more than child fields or a child that a cast reads per index.
     */
    @Nullable
    public Expression childPredicate() {
        Map<NameId, FieldAttribute> fields = new HashMap<>();
        List<String> names = fieldNames();
        for (int i = 0; i < inputs.size(); i++) {
            Attribute input = inputs.get(i);
            if (EsPhysicalOperationProviders.findUnionTypes(input) != null) {
                return null;
            }
            EsField field = new EsField(names.get(i), input.dataType(), Map.of(), true, EsField.TimeSeriesFieldType.NONE);
            fields.put(input.id(), new FieldAttribute(input.source(), names.get(i), field));
        }
        if (body.references().stream().anyMatch(attribute -> fields.containsKey(attribute.id()) == false)) {
            return null;
        }
        return body.transformUp(Attribute.class, attribute -> fields.get(attribute.id()));
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
        return NodeInfo.create(this, NestedPredicate::new, path, body, inputs, relativePaths, queryBuilder, configuration());
    }

    @Override
    public NestedPredicate replaceChildren(List<Expression> newChildren) {
        return new NestedPredicate(source(), path, newChildren.getFirst(), inputs, relativePaths, queryBuilder, configuration());
    }

    /** The inputs are loaded from the child documents by {@link NestedChildren}, so they are not columns of the parent row. */
    @Override
    public AttributeSet references() {
        return body.references().subtract(AttributeSet.of(inputs));
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj)
            && obj instanceof NestedPredicate other
            && path.equals(other.path)
            && body.equals(other.body)
            && inputs.equals(other.inputs)
            && relativePaths.equals(other.relativePaths)
            && Objects.equals(queryBuilder, other.queryBuilder);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), path, body, inputs, relativePaths, queryBuilder);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(EvaluatorMapper.ToEvaluator toEvaluator) {
        if (queryBuilder != null) {
            return new LuceneQueryExpressionEvaluator.Factory(
                toEvaluator.shardContexts().map(shard -> new ShardConfig(shard.toQuery(queryBuilder), shard.searcher()))
            );
        }
        Layout layout = new Layout.Builder().append(inputs).build();
        FoldContext foldCtx = toEvaluator.foldCtx();
        ExpressionEvaluator.Factory predicate = EvalMapper.toEvaluator(foldCtx, body, layout, toEvaluator.shardContexts(), null);
        List<DataType> types = inputs.stream().map(Attribute::dataType).toList();
        NestedChildren.Factory children = NestedChildren.factory(
            toEvaluator.shardContexts(),
            path,
            relativePaths,
            types,
            inputs.stream().map(EsPhysicalOperationProviders::findUnionTypes).toList(),
            toEvaluator.plannerSettings(),
            configuration()
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
                return new NestedPredicateEvaluator(context, read, eval, fieldChannels);
            }
        };
    }
}
