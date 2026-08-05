/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.querydsl.query.NestedQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.NestedEsField;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/**
 * {@code NESTED_ANY(field, u -> predicate)} — true when at least one object in the {@code nested} field
 * {@code field} satisfies {@code predicate}. The predicate is evaluated against one nested object at a
 * time and may only reference that object's sub-fields (via the lambda parameter, e.g. {@code u.role}).
 * <p>
 * It is a WHERE-only, translate-only function (like the other {@link FullTextFunction}s): the predicate is
 * translated at planning time into an Elasticsearch {@code nested} query (see {@link NestedQuery}), never
 * evaluated per row. The base {@code query} child holds the nested {@code field}; the lambda predicate is
 * carried as the second child. Its sub-field references are resolved against the nested field's properties
 * (see {@code Analyzer} lambda-scoped resolution) and are internal to the nested scope, so they do not leak
 * as outer references (see {@link #references()}).
 */
public class NestedAny extends FullTextFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "NestedAny",
        NestedAny::readFrom
    );

    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(NestedAny.class).binary(NestedAny::new).name("nested_any");

    private final Expression predicate;

    @FunctionInfo(
        returnType = "boolean",
        preview = true,
        description = "Returns true when at least one object in a nested field matches the given predicate."
    )
    public NestedAny(
        Source source,
        @Param(name = "field", type = { "nested" }, description = "Nested field whose objects are tested.") Expression field,
        Expression predicate
    ) {
        this(source, field, predicate, null);
    }

    public NestedAny(Source source, Expression field, Expression predicate, QueryBuilder queryBuilder) {
        super(source, field, predicate == null ? List.of(field) : List.of(field, predicate), queryBuilder);
        this.predicate = predicate;
    }

    private static NestedAny readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression predicate = in.readNamedWriteable(Expression.class);
        QueryBuilder queryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        return new NestedAny(source, field, predicate, queryBuilder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(query()); // the nested field
        out.writeNamedWriteable(predicate);
        out.writeOptionalNamedWriteable(queryBuilder());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    /** The nested field being tested (the base {@code query} child). */
    public Expression field() {
        return query();
    }

    /**
     * The predicate applied to each nested object. Parsed as a {@code Lambda}; after lambda-scoped
     * resolution (see {@code Analyzer}) this is the bound boolean body over the field's sub-fields.
     */
    public Expression predicate() {
        return predicate;
    }

    @Override
    protected TypeResolution resolveParams() {
        // resolveType (final) only calls this once children are resolved, i.e. after the lambda has been
        // bound to the nested field and `predicate` is the resolved boolean body.
        if ((field() instanceof FieldAttribute fa && fa.field() instanceof NestedEsField) == false) {
            return new TypeResolution(format(null, "first argument of [{}] must be a nested field", sourceText()));
        }
        FieldAttribute fieldAttr = (FieldAttribute) field();
        if (predicate.dataType() != DataType.BOOLEAN) {
            return new TypeResolution(
                format(null, "predicate of [{}] must be a boolean expression, found [{}]", sourceText(), predicate.dataType().typeName())
            );
        }
        String prefix = fieldAttr.name() + ".";
        Holder<TypeResolution> failure = new Holder<>();
        predicate.forEachDown(FieldAttribute.class, ref -> {
            if (failure.get() != null) {
                return;
            }
            // Every reference in the predicate must be a sub-field of the nested field. This also guards
            // against the generic resolution pass having resolved a cross-scope reference (e.g. a top-level
            // column) that would otherwise be translated, incorrectly, inside the nested query.
            if (ref.name().startsWith(prefix) == false) {
                failure.set(
                    new TypeResolution(
                        format(
                            null,
                            "predicate of [{}] may only reference sub-fields of [{}], found [{}]",
                            sourceText(),
                            fieldAttr.name(),
                            ref.name()
                        )
                    )
                );
                return;
            }
            // The predicate is only ever executed as a Lucene query (pushed to the source, or run by a
            // LuceneQueryExpressionEvaluator); there is no per-nested-object compute path. So a sub-field
            // that cannot produce an exact-match query — e.g. a text field with no keyword multi-field —
            // can never be evaluated, and must be rejected here rather than throwing during translation.
            // This condition is deliberately independent of SearchStats: LucenePushdownPredicates.DEFAULT
            // would also reject text fields that *do* have a usable keyword multi-field.
            if (ref.getExactInfo().hasExact() == false) {
                failure.set(
                    new TypeResolution(
                        format(null, "predicate of [{}] cannot be pushed to Lucene: {}", sourceText(), ref.getExactInfo().errorMsg())
                    )
                );
            }
        });
        return failure.get() != null ? failure.get() : TypeResolution.TYPE_RESOLVED;
    }

    /**
     * Pushable only when the inner predicate is: the nested query wraps the predicate's translation, so an
     * untranslatable predicate makes the whole function untranslatable. When this reports {@code NO} the
     * clause stays in the {@code FilterExec} and is executed by a {@link org.elasticsearch.compute.lucene
     * .query.LuceneQueryExpressionEvaluator} over the {@code nested} query that the coordinator already
     * materialized — the same fallback the other full-text functions use, e.g. for a disjunction.
     */
    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        return super.translatable(pushdownPredicates).merge(TranslationAware.translatable(predicate, pushdownPredicates));
    }

    @Override
    protected Query translate(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        String path = ((FieldAttribute) field()).name();
        // Defensive: resolveParams already rejects a predicate that cannot produce an exact-match query, so
        // this should be unreachable. It guards the unguarded asQuery call in QueryBuilderResolver, which
        // would otherwise surface as a raw QlIllegalArgumentException from deep inside translation.
        predicate.forEachDown(FieldAttribute.class, ref -> {
            if (ref.getExactInfo().hasExact() == false) {
                throw new EsqlIllegalArgumentException(
                    format(null, "predicate of [{}] cannot be pushed to Lucene: {}", sourceText(), ref.getExactInfo().errorMsg())
                );
            }
        });
        // Translate through the handler so each leaf predicate keeps its SingleValueQuery wrapper: a nested
        // object's sub-field may itself be multi-valued, and ES|QL comparison semantics treat a multi-valued
        // field as no-match. The wrapper enforces that per child document, inside the nested scope.
        Query inner = handler.asQuery(pushdownPredicates, predicate);
        return new NestedQuery(source(), path, inner, NestedQuery.DEFAULT_SCORE_MODE);
    }

    /**
     * The nested field and the predicate's sub-field references are internal to the nested scope — they are
     * resolved from the index mapping, not from input rows — so this expression has no outer references.
     * Reporting them would fail the verifier's "unresolved/missing reference" check.
     */
    @Override
    public AttributeSet references() {
        return AttributeSet.EMPTY;
    }

    @Override
    public void postOptimizationVerification(Failures failures) {
        // Base implementation validates a foldable string query; NESTED_ANY's query child is the nested
        // field, so skip it. The predicate is validated during analysis and translated at push-down.
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new NestedAny(source(), newChildren.get(0), newChildren.size() > 1 ? newChildren.get(1) : null, queryBuilder());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, NestedAny::new, field(), predicate, queryBuilder());
    }

    @Override
    public Expression replaceQueryBuilder(QueryBuilder queryBuilder) {
        return new NestedAny(source(), field(), predicate, queryBuilder);
    }

    // equals/hashCode are inherited: Function#equals compares all children (field + predicate) and
    // FullTextFunction adds the queryBuilder, so no override is needed here.
}
