/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.AnalyzedTextExpression;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.RuntimeBm25;
import org.elasticsearch.xpack.esql.expression.function.fulltext.RuntimeBm25Field;
import org.elasticsearch.xpack.esql.expression.function.fulltext.RuntimeTermStat;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToText;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;

/**
 * Scores a runtime {@code MATCH} on a column declaring {@code TO_TEXT(..., {"similarity": "bm25"})} with BM25, by computing the
 * collection statistics BM25 needs in a separate pass over the rows entering the {@code WHERE}:
 * <pre>{@code
 * FROM idx METADATA _score | WHERE MATCH(t, "fox dog")
 * }</pre>
 * becomes, conceptually,
 * <pre>{@code
 * FROM idx METADATA _score
 * | INLINE STATS $$n = COUNT($$len), $$sum = SUM($$len), $$df_fox = COUNT($$tf_fox), $$df_dog = COUNT($$tf_dog)
 * | WHERE MATCH(RuntimeBm25Field(t, $$n, $$sum, $$df_fox, $$df_dog), "fox dog")
 * | KEEP <the original columns>
 * }</pre>
 * where {@code $$len} and {@code $$tf_*} are {@link RuntimeTermStat}s of {@code t}. {@code INLINE STATS} runs its aggregation as a
 * subplan first and inlines the one-row result as literals, so the {@code MATCH} scorer reads the statistics as constant columns.
 * <p>
 * The cost is that everything below the {@code WHERE} executes twice. The query terms are analyzed here, on the coordinator, so the
 * number of statistics is known when planning.
 * <p>
 * Proof of concept limitations, where the {@code MATCH} keeps scoring with the boolean similarity instead:
 * <ul>
 *     <li>{@code MATCH} with options (the Lucene-query path: operator, fuzziness, boost, ...);</li>
 *     <li>a query that is not a literal yet when this rule runs;</li>
 *     <li>a {@code LIMIT}, {@code SORT} or {@code SAMPLE} below the {@code WHERE}: {@code INLINE STATS} does not support them, since
 *     its two executions could see different rows;</li>
 *     <li>no analysis registry, when the column declares an analyzer other than the default.</li>
 * </ul>
 */
public final class RewriteRuntimeMatchBm25 extends OptimizerRules.ParameterizedOptimizerRule<Filter, LogicalOptimizerContext> {

    public RewriteRuntimeMatchBm25() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(Filter filter, LogicalOptimizerContext context) {
        if (PlannerUtils.usesScoring(filter) == false || executesDifferentlyTwice(filter.child())) {
            return filter;
        }
        List<Alias> stats = new ArrayList<>();
        Expression condition = filter.condition()
            .transformUp(Match.class, match -> rewrite(match, filter.child(), context.analysisRegistry(), stats));
        if (stats.isEmpty()) {
            return filter;
        }
        Source source = filter.source();
        InlineStats inlineStats = new InlineStats(source, new Aggregate(source, filter.child(), List.of(), stats));
        // Drop the statistics columns again: they are an implementation detail of the scorer.
        return new Project(source, new Filter(source, inlineStats, condition), filter.output());
    }

    private static boolean executesDifferentlyTwice(LogicalPlan plan) {
        return plan.anyMatch(
            p -> p instanceof Limit || p instanceof OrderBy || p instanceof TopN || p instanceof Sample || p instanceof Fork
        );
    }

    private static Expression rewrite(Match match, LogicalPlan child, AnalysisRegistry registry, List<Alias> stats) {
        Expression field = match.field();
        if (field instanceof RuntimeBm25Field
            || match.options() != null
            || field.dataType() != TEXT
            || match.isRuntimeSearch() == false
            || match.query() instanceof Literal == false
            || declaresBm25(field, child) == false) {
            return match;
        }
        String analyzerName = AnalyzedTextExpression.valuesAnalyzerOf(field);
        if (analyzerName != null && registry == null) {
            return match;
        }
        Analyzer analyzer = analyzerName == null ? new StandardAnalyzer() : PlannerUtils.resolveAnalyzer(analyzerName, registry);
        Map<BytesRef, Integer> queryTerms = RuntimeBm25.analyzeQuery(analyzer, BytesRefs.toString(((Literal) match.query()).value()));
        if (queryTerms.isEmpty()) {
            return match;
        }

        Source source = match.source();
        String prefix = "$$bm25_" + stats.size() + "_";
        RuntimeTermStat length = new RuntimeTermStat(source, field, null);
        Alias docCount = statistic(source, prefix + "doc_count", new Count(source, length));
        Alias sumLength = statistic(source, prefix + "sum_length", new Sum(source, length));
        stats.add(docCount);
        stats.add(sumLength);

        List<BytesRef> terms = new ArrayList<>(queryTerms.size());
        List<Integer> weights = new ArrayList<>(queryTerms.size());
        List<Attribute> docFreqs = new ArrayList<>(queryTerms.size());
        for (Map.Entry<BytesRef, Integer> term : queryTerms.entrySet()) {
            Alias docFreq = statistic(
                source,
                prefix + "doc_freq_" + terms.size(),
                new Count(source, new RuntimeTermStat(source, field, term.getKey()))
            );
            stats.add(docFreq);
            docFreqs.add(docFreq.toAttribute());
            terms.add(term.getKey());
            weights.add(term.getValue());
        }
        RuntimeBm25Field bm25Field = new RuntimeBm25Field(
            source,
            field,
            docCount.toAttribute(),
            sumLength.toAttribute(),
            docFreqs,
            terms,
            weights
        );
        return match.replaceChildren(List.of(bm25Field, match.query()));
    }

    private static Alias statistic(Source source, String name, Expression aggregate) {
        return new Alias(source, name, aggregate, null, true);
    }

    /**
     * Whether {@code field} is a {@code TO_TEXT} declaring BM25, possibly through {@code EVAL} aliases and {@code RENAME}s in
     * {@code plan}. The declaration only matters here, on the coordinator: after this rule the {@link RuntimeBm25Field} carries it.
     */
    private static boolean declaresBm25(Expression field, LogicalPlan plan) {
        if (field instanceof ToText toText) {
            return toText.isBm25Similarity();
        }
        if (field instanceof Attribute == false) {
            return false;
        }
        Holder<Attribute> current = new Holder<>((Attribute) field);
        Holder<Boolean> bm25 = new Holder<>(false);
        plan.forEachDownMayReturnEarly((p, breakEarly) -> {
            List<? extends NamedExpression> aliases;
            if (p instanceof Eval eval) {
                aliases = eval.fields();
            } else if (p instanceof Project project) {
                aliases = project.projections();
            } else {
                return;
            }
            for (NamedExpression ne : aliases) {
                if (ne instanceof Alias alias && alias.id().equals(current.get().id())) {
                    if (alias.child() instanceof ToText toText) {
                        bm25.set(toText.isBm25Similarity());
                        breakEarly.set(true);
                    } else if (alias.child() instanceof Attribute next) {
                        current.set(next);
                    } else {
                        breakEarly.set(true);
                    }
                    return;
                }
            }
        });
        return bm25.get();
    }
}
