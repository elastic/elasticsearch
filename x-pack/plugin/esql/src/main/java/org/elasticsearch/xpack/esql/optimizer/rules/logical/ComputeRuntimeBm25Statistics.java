/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.fulltext.RuntimeBm25Field;
import org.elasticsearch.xpack.esql.expression.function.fulltext.RuntimeBm25RowStats;
import org.elasticsearch.xpack.esql.expression.function.fulltext.RuntimeBm25Support;
import org.elasticsearch.xpack.esql.expression.function.fulltext.SingleFieldFullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSlice;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

/**
 * Computes the BM25 statistics of {@code MATCH} and {@code MATCH_PHRASE} calls with {@code similarity_function: bm25}
 * on text that has no index behind it, so that the planner can score them with Lucene.
 * <p>
 * It runs first in the {@code substitutions()} batch, on the analyzed plan, after the Verifier has rejected every
 * shape Stage 1 does not support. For the stretch of the query around the BM25 calls (see {@link RuntimeBm25Support})
 * whose {@code _score} the planner will compute, it:
 * <ol>
 *     <li>removes the runtime {@code MATCH} and {@code MATCH_PHRASE} conditions, and conditions reading {@code _score},
 *     from the stretch's filters, so the remaining conditions narrow the statistics rows;</li>
 *     <li>adds, at the end of the stretch, an {@link InlineJoin} whose right side is
 *     {@code Aggregate[SUM(MV_SLICE($$bm25_row_g, i, i))]} over one {@code Eval[$$bm25_row_g = RUNTIME_BM25_ROW_STATS(...)]}
 *     per statistics group, over the stub of the stretch;</li>
 *     <li>re-applies the removed conditions above the join, in a fixed order, with the field of each BM25 call wrapped
 *     in a {@link RuntimeBm25Field} that references its group's statistics columns;</li>
 *     <li>restores the stretch's output with a {@link Project}, so the statistics columns never reach the user.</li>
 * </ol>
 * It builds the join itself instead of emitting {@code INLINE STATS}: {@code ReplaceAggregateNestedExpressionWithEval}
 * deduplicates only whole aggregate inputs, so each {@code MV_SLICE(RUNTIME_BM25_ROW_STATS(...), i, i)} would get its
 * own {@code Eval} column and analyze every row k + 2 times. The join has the shape {@code INLINE STATS} takes after its
 * own rules, so every {@code InlineJoin} rule still applies. A statistics group is a distinct pair of field expression
 * and values analyzer.
 */
public final class ComputeRuntimeBm25Statistics extends ParameterizedRule<LogicalPlan, LogicalPlan, LogicalOptimizerContext> {

    private static final String STANDARD = "standard";

    @Override
    public LogicalPlan apply(LogicalPlan plan, LogicalOptimizerContext context) {
        RuntimeBm25Support.Stretch stretch = RuntimeBm25Support.findBm25Stretch(plan);
        if (stretch == null || stretch.scoreIsRead() == false) {
            // No BM25 call, or nothing scores it: it filters exactly as a boolean MATCH does.
            return plan;
        }
        AnalysisRegistry registry = context.analysisRegistry();
        if (registry == null) {
            throw new IllegalStateException("BM25 scoring of runtime text needs the analysis registry in the optimizer context");
        }
        LogicalPlan replacement = rewrite(stretch, registry);
        LogicalPlan top = stretch.top();
        return plan.transformDown(LogicalPlan.class, node -> node == top ? replacement : node);
    }

    private static LogicalPlan rewrite(RuntimeBm25Support.Stretch stretch, AnalysisRegistry registry) {
        // 1. Remove the lifted conditions from the stretch's filters, rebuilding it from the bottom up.
        List<Expression> lifted = new ArrayList<>();
        Source filterSource = stretch.top().source();
        LogicalPlan current = stretch.start();
        for (LogicalPlan node : stretch.nodes()) {
            if (node instanceof Filter filter) {
                List<Expression> kept = new ArrayList<>();
                for (Expression conjunct : Predicates.splitAnd(filter.condition())) {
                    (RuntimeBm25Support.isLifted(conjunct) ? lifted : kept).add(conjunct);
                }
                if (RuntimeBm25Support.holdsUnpreparedBm25(filter.condition())) {
                    filterSource = filter.source();
                }
                // A filter left with no conditions is removed.
                current = kept.isEmpty() ? current : filter.with(current, Predicates.combineAnd(kept));
            } else {
                current = ((UnaryPlan) node).replaceChild(current);
            }
        }
        LogicalPlan stretchWithoutSearches = current;
        Source source = stretch.top().source();

        // 2. Group the BM25 calls by field and values analyzer, and collect each group's terms.
        List<Group> groups = new ArrayList<>();
        for (Expression conjunct : lifted) {
            conjunct.forEachDown(SingleFieldFullTextFunction.class, function -> {
                if (RuntimeBm25Support.isBm25(function) && function.field() instanceof RuntimeBm25Field == false) {
                    group(groups, function).terms.addAll(RuntimeBm25Support.collectTerms(function, registry));
                }
            });
        }
        List<Alias> rowStats = new ArrayList<>(groups.size());
        List<Alias> aggregates = new ArrayList<>();
        for (int g = 0; g < groups.size(); g++) {
            groups.get(g).close(source, g + 1, rowStats, aggregates);
        }

        // 3. The statistics join, in the shape INLINE STATS takes after its own rules.
        Eval perRow = new Eval(source, stretchWithoutSearches, rowStats);
        LogicalPlan stubbed = InlineJoin.stubSource(perRow, stretchWithoutSearches);
        Aggregate statistics = new Aggregate(source, stubbed, List.of(), aggregates);
        InlineJoin join = new InlineJoin(
            source,
            stretchWithoutSearches,
            statistics,
            new JoinConfig(JoinTypes.LEFT, List.of(), List.of(), null)
        );

        // 4. Re-apply the lifted conditions in a fixed order, with the BM25 fields wrapped.
        List<Expression> ordered = lifted.stream().sorted(LIFTED_ORDER).map(conjunct -> wrapFields(conjunct, groups)).toList();
        Filter searches = new Filter(filterSource, join, Predicates.combineAnd(ordered));

        // 5. Restore the stretch's output.
        return new Project(source, searches, stretch.top().output());
    }

    /**
     * The order the lifted conditions are re-applied in. {@code _score} sums each condition's score, and float addition
     * of three or more values depends on their order, so equivalent queries must get the same order. Single calls come
     * first, by function, query, options and finally field, so that an alias reorders nothing unless two calls differ
     * only in their field. Other conditions follow, by their source text.
     */
    private static final Comparator<Expression> LIFTED_ORDER = Comparator.comparing((Expression e) -> isSingleCall(e) ? 0 : 1)
        .thenComparing(e -> isSingleCall(e) ? ((SingleFieldFullTextFunction) e).functionName() : "")
        .thenComparing(e -> isSingleCall(e) ? ((SingleFieldFullTextFunction) e).query().sourceText() : "")
        .thenComparing(e -> isSingleCall(e) ? optionsText((SingleFieldFullTextFunction) e) : "")
        .thenComparing(e -> isSingleCall(e) ? ((SingleFieldFullTextFunction) e).field().sourceText() : e.sourceText());

    private static boolean isSingleCall(Expression conjunct) {
        return conjunct instanceof SingleFieldFullTextFunction && RuntimeBm25Support.isRuntimeMatch(conjunct);
    }

    private static String optionsText(SingleFieldFullTextFunction function) {
        return function.options() instanceof MapExpression options ? options.sourceText() : "";
    }

    private static Expression wrapFields(Expression conjunct, List<Group> groups) {
        return conjunct.transformUp(SingleFieldFullTextFunction.class, function -> {
            if (RuntimeBm25Support.isBm25(function) == false || function.field() instanceof RuntimeBm25Field) {
                return function;
            }
            Group group = group(groups, function);
            List<Expression> children = new ArrayList<>(function.children());
            children.set(0, new RuntimeBm25Field(function.field().source(), function.field(), group.termsLiteral, group.statistics));
            return function.replaceChildren(children);
        });
    }

    private static Group group(List<Group> groups, SingleFieldFullTextFunction function) {
        String analyzer = Objects.requireNonNullElse(RuntimeBm25Support.valuesAnalyzerName(function), STANDARD);
        for (Group group : groups) {
            if (group.analyzer.equals(analyzer) && group.field.semanticEquals(function.field())) {
                return group;
            }
        }
        Group group = new Group(function.field(), analyzer);
        groups.add(group);
        return group;
    }

    /** The BM25 calls on one field expression with one values analyzer share one statistics group. */
    private static final class Group {
        private final Expression field;
        private final String analyzer;
        private final TreeSet<BytesRef> terms = new TreeSet<>();
        private Literal termsLiteral;
        private List<Expression> statistics;

        Group(Expression field, String analyzer) {
            this.field = field;
            this.analyzer = analyzer;
        }

        /** Adds this group's per-row statistics column and its k + 2 sums, named after the group number. */
        void close(Source source, int number, List<Alias> rowStats, List<Alias> aggregates) {
            if (terms.size() > IndexSearcher.getMaxClauseCount()) {
                throw new InvalidArgumentException(
                    "the BM25 conditions on [{}] analyze to [{}] distinct terms; at most [{}] are allowed",
                    field.sourceText(),
                    terms.size(),
                    IndexSearcher.getMaxClauseCount()
                );
            }
            List<BytesRef> sortedTerms = List.copyOf(terms);
            termsLiteral = RuntimeBm25RowStats.termsLiteral(source, sortedTerms);
            Alias row = new Alias(
                source,
                "$$bm25_row_" + number,
                new RuntimeBm25RowStats(
                    source,
                    field,
                    STANDARD.equals(analyzer) ? new Literal(source, null, DataType.KEYWORD) : Literal.keyword(source, analyzer),
                    termsLiteral
                )
            );
            rowStats.add(row);
            Attribute rowAttribute = row.toAttribute();
            List<Expression> columns = new ArrayList<>(sortedTerms.size() + 2);
            for (int i = 0; i < sortedTerms.size() + 2; i++) {
                String name = switch (i) {
                    case 0 -> "$$bm25_n_" + number;
                    case 1 -> "$$bm25_len_" + number;
                    default -> "$$bm25_df_" + number + "_" + (i - 2);
                };
                Literal position = Literal.integer(source, i);
                Alias sum = new Alias(source, name, new Sum(source, new MvSlice(source, rowAttribute, position, position)));
                aggregates.add(sum);
                columns.add(sum.toAttribute());
            }
            statistics = columns;
        }
    }
}
