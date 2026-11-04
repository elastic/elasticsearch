/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.AnalyzedTextExpression;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.expression.function.fulltext.RuntimeBm25RowStats.ESQL_RUNTIME_MATCH_BM25;

/**
 * Wraps the field of a {@code MATCH} or {@code MATCH_PHRASE} with {@code similarity_function: bm25} once the
 * optimizer has computed its BM25 statistics. It carries the group's term list and references to the k + 2
 * statistics columns (N, the summed length and one df per term), which the scorer reads.
 * <p>
 * It evaluates to the wrapped field's value, so filtering is unchanged, and it declares the wrapped field's values
 * analyzer. It is never a field attribute, so the function stays a runtime search. It is never foldable: folding it
 * on a data node would drop the statistics and leave the function to score with boolean similarity.
 */
public class RuntimeBm25Field extends EsqlScalarFunction implements AnalyzedTextExpression {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "RuntimeBm25Field",
        RuntimeBm25Field::new
    );

    private final Expression field;
    private final Expression terms;
    private final List<Expression> statistics;

    /**
     * @param field      the wrapped field
     * @param terms      the group's sorted term list, built with {@link RuntimeBm25RowStats#termsLiteral}
     * @param statistics N, the summed length and one df per term, in that order
     */
    public RuntimeBm25Field(Source source, Expression field, Expression terms, List<Expression> statistics) {
        super(source, children(field, terms, statistics));
        this.field = field;
        this.terms = terms;
        this.statistics = List.copyOf(statistics);
    }

    private RuntimeBm25Field(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
    }

    private static List<Expression> children(Expression field, Expression terms, List<Expression> statistics) {
        List<Expression> children = new ArrayList<>(statistics.size() + 2);
        children.add(field);
        children.add(terms);
        children.addAll(statistics);
        return children;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(ESQL_RUNTIME_MATCH_BM25) == false) {
            // IllegalArgumentException returns a 400. The coordinator's Verifier rejects such queries earlier.
            throw new IllegalArgumentException(
                "BM25 scoring of ["
                    + field.sourceText()
                    + "] is not supported in peer node's version ["
                    + out.getTransportVersion()
                    + "]. Upgrade to version ["
                    + ESQL_RUNTIME_MATCH_BM25
                    + "] or newer."
            );
        }
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeNamedWriteable(terms);
        out.writeNamedWriteableCollection(statistics);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Expression field() {
        return field;
    }

    public Expression terms() {
        return terms;
    }

    public List<Expression> statistics() {
        return statistics;
    }

    /** The term list carried by {@link #terms}, in its order. */
    public List<BytesRef> termList(FoldContext foldContext) {
        Object folded = terms.fold(foldContext);
        if (folded == null) {
            return List.of();
        }
        if (folded instanceof List<?> list) {
            return list.stream().map(BytesRefs::toBytesRef).toList();
        }
        return List.of(BytesRefs.toBytesRef(folded));
    }

    @Override
    public String valuesAnalyzer() {
        return AnalyzedTextExpression.valuesAnalyzerOf(field);
    }

    @Override
    public DataType dataType() {
        return field.dataType();
    }

    @Override
    public Nullability nullable() {
        return field.nullable();
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
    public Expression replaceChildren(List<Expression> newChildren) {
        return new RuntimeBm25Field(source(), newChildren.get(0), newChildren.get(1), newChildren.subList(2, newChildren.size()));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, RuntimeBm25Field::new, field, terms, statistics);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return toEvaluator.apply(field);
    }
}
