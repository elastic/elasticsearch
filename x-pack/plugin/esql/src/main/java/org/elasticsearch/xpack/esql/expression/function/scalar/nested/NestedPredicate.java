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
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
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
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link NestedAny} lowered to one boolean per parent. Its children are the predicate body and the outer expressions: the
 * parts of the predicate that read parent columns and no child field. Each outer expression is evaluated once per parent and
 * every child sees that value as an input of the body. The body's child fields are loaded from the child documents, and the
 * parent {@code _doc} stays on the page through
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
    /** Where {@link #body} reads the value of each of {@link #outers}, in the same order. */
    private final List<Attribute> outerInputs;
    private final List<Expression> outers;
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
        List<Attribute> outerInputs,
        List<Expression> outers,
        @Nullable QueryBuilder queryBuilder,
        Configuration configuration
    ) {
        super(source, children(body, outers), configuration);
        this.path = path;
        this.body = body;
        this.inputs = List.copyOf(inputs);
        this.relativePaths = List.copyOf(relativePaths);
        this.outerInputs = List.copyOf(outerInputs);
        this.outers = List.copyOf(outers);
        this.queryBuilder = queryBuilder;
    }

    private static List<Expression> children(Expression body, List<Expression> outers) {
        List<Expression> children = new ArrayList<>(1 + outers.size());
        children.add(body);
        children.addAll(outers);
        return children;
    }

    private NestedPredicate(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readString(),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readStringCollectionAsList(),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readNamedWriteableCollectionAsList(Expression.class),
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
        out.writeNamedWriteableCollection(outerInputs);
        out.writeNamedWriteableCollection(outers);
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

    /** The parts of the predicate that read parent columns and no child field, each evaluated once per parent. */
    public List<Expression> outers() {
        return outers;
    }

    public List<Attribute> outerInputs() {
        return outerInputs;
    }

    @Nullable
    public QueryBuilder queryBuilder() {
        return queryBuilder;
    }

    public NestedPredicate withQueryBuilder(QueryBuilder queryBuilder) {
        return new NestedPredicate(source(), path, body, inputs, relativePaths, outerInputs, outers, queryBuilder, configuration());
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
        Expression effective = bodyWithConstantOuters();
        if (effective.references().stream().anyMatch(attribute -> fields.containsKey(attribute.id()) == false)) {
            return null;
        }
        return effective.transformUp(Attribute.class, attribute -> fields.get(attribute.id()));
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
        return NodeInfo.create(
            this,
            NestedPredicate::new,
            path,
            body,
            inputs,
            relativePaths,
            outerInputs,
            outers,
            queryBuilder,
            configuration()
        );
    }

    @Override
    public NestedPredicate replaceChildren(List<Expression> newChildren) {
        return new NestedPredicate(
            source(),
            path,
            newChildren.getFirst(),
            inputs,
            relativePaths,
            outerInputs,
            newChildren.subList(1, newChildren.size()),
            queryBuilder,
            configuration()
        );
    }

    /** The body, with each outer expression that a rewrite folded to a constant in its place. */
    private Expression bodyWithConstantOuters() {
        Map<NameId, Expression> constants = new HashMap<>();
        for (int i = 0; i < Math.min(outerInputs.size(), outers.size()); i++) {
            if (outers.get(i) instanceof Literal) {
                constants.put(outerInputs.get(i).id(), outers.get(i));
            }
        }
        return constants.isEmpty()
            ? body
            : body.transformUp(Attribute.class, attribute -> constants.getOrDefault(attribute.id(), attribute));
    }

    /**
     * The parent columns the outer expressions read. The inputs are loaded from the child documents by {@link NestedChildren},
     * so they are not columns of the parent row.
     */
    @Override
    public AttributeSet references() {
        return Expressions.references(outers)
            .combine(body.references().subtract(AttributeSet.of(inputs)).subtract(AttributeSet.of(outerInputs)));
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj)
            && obj instanceof NestedPredicate other
            && path.equals(other.path)
            && body.equals(other.body)
            && inputs.equals(other.inputs)
            && relativePaths.equals(other.relativePaths)
            && outerInputs.equals(other.outerInputs)
            && outers.equals(other.outers)
            && Objects.equals(queryBuilder, other.queryBuilder);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), path, body, inputs, relativePaths, outerInputs, outers, queryBuilder);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(EvaluatorMapper.ToEvaluator toEvaluator) {
        if (queryBuilder != null) {
            return new LuceneQueryExpressionEvaluator.Factory(
                toEvaluator.shardContexts().map(shard -> new ShardConfig(shard.toQuery(queryBuilder), shard.searcher()))
            );
        }
        Layout layout = new Layout.Builder().append(inputs).append(outerInputs).build();
        FoldContext foldCtx = toEvaluator.foldCtx();
        List<ExpressionEvaluator.Factory> outerEvaluators = outers.stream().map(toEvaluator::apply).toList();
        ExpressionEvaluator.Factory predicate = EvalMapper.toEvaluator(foldCtx, body, layout, toEvaluator.shardContexts(), null);
        List<DataType> types = inputs.stream().map(Attribute::dataType).toList();
        // The expand command threads the query's planner settings. A predicate uses the defaults: the budgets change how
        // large a child page may be, not the values a small nested object reads.
        NestedChildren.Factory children = NestedChildren.factory(
            toEvaluator.shardContexts(),
            path,
            relativePaths,
            types,
            inputs.stream().map(EsPhysicalOperationProviders::findUnionTypes).toList(),
            PlannerSettings.DEFAULTS,
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
                ExpressionEvaluator[] outer = new ExpressionEvaluator[outerEvaluators.size()];
                for (int i = 0; i < outer.length; i++) {
                    outer[i] = outerEvaluators.get(i).get(context);
                }
                return new NestedPredicateEvaluator(context, read, eval, outer, fieldChannels);
            }
        };
    }
}
