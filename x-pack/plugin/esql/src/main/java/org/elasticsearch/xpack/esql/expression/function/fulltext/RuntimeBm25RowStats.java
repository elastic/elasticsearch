/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * The per-row half of the BM25 statistics pass for {@code MATCH} and {@code MATCH_PHRASE} with
 * {@code similarity_function: bm25} on text that has no index behind it.
 * <p>
 * For each row it analyzes all values once, as one document, and returns a multi-valued {@code LONG}:
 * {@code [hasToken, tokenCount, present(term 0), ..., present(term k-1)]}, in the order of the given term list.
 * The statistics pass adds up each position with {@code SUM(MV_SLICE(row, i, i))}, which gives the numbers an index
 * of the same rows would hold: N ({@code docCount}), the summed length ({@code sumTotalTermFreq}) and the df of each
 * term ({@code docFreq}). The token count includes stacked tokens such as synonyms, as {@code sumTotalTermFreq} does.
 * A null row returns null, and {@code SUM} skips it.
 * <p>
 * The analysis is a scalar, not an aggregation, because evaluators can resolve analyzers through the node's
 * analysis registry and aggregators cannot. One call analyzes the row once whatever the number of terms.
 * <p>
 * This function has no user-facing name and is not in the function registry: the optimizer creates it.
 */
public class RuntimeBm25RowStats extends EsqlScalarFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "RuntimeBm25RowStats",
        RuntimeBm25RowStats::new
    );

    /** Gates every element that BM25 scoring of runtime text adds to the plan. */
    public static final TransportVersion ESQL_RUNTIME_MATCH_BM25 = TransportVersion.fromName("esql_runtime_match_bm25");

    private final Expression field;
    private final Expression analyzer;
    private final Expression terms;

    /**
     * @param field    the text to analyze
     * @param analyzer a keyword literal naming the values analyzer; a null value means {@code standard}
     * @param terms    a literal with the sorted query terms, built with {@link #termsLiteral}
     */
    public RuntimeBm25RowStats(Source source, Expression field, Expression analyzer, Expression terms) {
        super(source, List.of(field, analyzer, terms));
        this.field = field;
        this.analyzer = analyzer;
        this.terms = terms;
    }

    private RuntimeBm25RowStats(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(ESQL_RUNTIME_MATCH_BM25) == false) {
            // IllegalArgumentException returns a 400. The coordinator's Verifier rejects such queries earlier.
            throw new IllegalArgumentException(
                "BM25 scoring of ["
                    + sourceText()
                    + "] is not supported in peer node's version ["
                    + out.getTransportVersion()
                    + "]. Upgrade to version ["
                    + ESQL_RUNTIME_MATCH_BM25
                    + "] or newer."
            );
        }
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeNamedWriteable(analyzer);
        out.writeNamedWriteable(terms);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    /**
     * Builds the literal that carries the sorted term list. An empty list becomes a null literal, because a query
     * that analyzes to no terms still needs N and the summed length.
     */
    public static Literal termsLiteral(Source source, List<BytesRef> sortedTerms) {
        return new Literal(source, sortedTerms.isEmpty() ? null : List.copyOf(sortedTerms), DataType.KEYWORD);
    }

    public Expression field() {
        return field;
    }

    public Expression analyzer() {
        return analyzer;
    }

    public Expression terms() {
        return terms;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isString(field, sourceText(), FIRST);
    }

    @Override
    public DataType dataType() {
        return DataType.LONG;
    }

    /**
     * Never foldable: evaluating it needs the analyzers of the node that runs the statistics pass.
     */
    @Override
    public boolean foldable() {
        return false;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new RuntimeBm25RowStats(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, RuntimeBm25RowStats::new, field, analyzer, terms);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        String analyzerName = BytesRefs.toString(analyzer.fold(toEvaluator.foldCtx()));
        Analyzer resolved = analyzerName == null ? Lucene.STANDARD_ANALYZER : RuntimeSearch.resolveNamedAnalyzer(analyzerName, toEvaluator);
        List<BytesRef> termList = termList(toEvaluator.foldCtx());
        Map<BytesRef, Integer> termIndex = new HashMap<>(termList.size());
        for (int i = 0; i < termList.size(); i++) {
            termIndex.put(termList.get(i), i);
        }
        int termCount = termList.size();
        return new RuntimeBm25RowStatsEvaluator.Factory(
            source(),
            toEvaluator.apply(field),
            resolved,
            termIndex,
            context -> new boolean[termCount],
            context -> new BytesRef()
        );
    }

    /** The term list carried by {@link #terms}, in its order. */
    List<BytesRef> termList(FoldContext foldContext) {
        Object folded = terms.fold(foldContext);
        if (folded == null) {
            return List.of();
        }
        if (folded instanceof List<?> list) {
            return list.stream().map(BytesRefs::toBytesRef).toList();
        }
        return List.of(BytesRefs.toBytesRef(folded));
    }

    /**
     * Analyzes every value of the row once and appends {@code [hasToken, tokenCount, present(term 0), ...]}.
     * Values are not sorted or deduplicated: position {@code i} must stay position {@code i}.
     */
    @Evaluator(warnExceptions = { IOException.class })
    static void process(
        LongBlock.Builder builder,
        @Position int position,
        BytesRefBlock field,
        @Fixed Analyzer analyzer,
        @Fixed Map<BytesRef, Integer> termIndex,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) boolean[] present,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) BytesRef scratch
    ) throws IOException {
        Arrays.fill(present, false);
        long tokenCount = 0;
        int first = field.getFirstValueIndex(position);
        int end = first + field.getValueCount(position);
        for (int valueIndex = first; valueIndex < end; valueIndex++) {
            scratch = field.getBytesRef(valueIndex, scratch);
            try (TokenStream stream = analyzer.tokenStream(RuntimeSearch.CONTENT_FIELD, scratch.utf8ToString())) {
                TermToBytesRefAttribute term = stream.addAttribute(TermToBytesRefAttribute.class);
                stream.reset();
                while (stream.incrementToken()) {
                    tokenCount++;
                    Integer index = termIndex.get(term.getBytesRef());
                    if (index != null) {
                        present[index] = true;
                    }
                }
                stream.end();
            }
        }
        builder.beginPositionEntry();
        builder.appendLong(tokenCount > 0 ? 1 : 0);
        builder.appendLong(tokenCount);
        for (boolean termPresent : present) {
            builder.appendLong(termPresent ? 1 : 0);
        }
        builder.endPositionEntry();
    }
}
