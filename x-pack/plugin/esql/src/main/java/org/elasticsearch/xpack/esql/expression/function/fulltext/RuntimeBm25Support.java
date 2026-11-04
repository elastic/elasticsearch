/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * The Stage 1 rules for BM25 scoring of {@code MATCH} and {@code MATCH_PHRASE} on text that has no index behind it,
 * shared by the Verifier, the optimizer rule {@code ComputeRuntimeBm25Statistics} and the two functions.
 * <p>
 * BM25 needs corpus statistics: N, the summed length and the df of each query term. They are computed over the
 * <em>stretch</em> of the query around the {@code MATCH}: from the nearest source, {@code STATS}, {@code INLINE STATS}
 * or {@code _score}-reading command below it, up to the first command after it that is not transparent, that reads
 * {@code _score}, or that drops a column the full-text conditions or the scoring need. Transparent commands
 * ({@code WHERE}, {@code EVAL}, {@code KEEP}, {@code DROP}, {@code RENAME}, {@code SORT} without a limit) keep every
 * row, so the statistics count the rows that pass the stretch's other conditions. Any other command before the
 * {@code MATCH} decides which rows exist, which a second read cannot repeat, so Stage 1 rejects it with a 400.
 */
public final class RuntimeBm25Support {

    private RuntimeBm25Support() {}

    // ---- Recognizing BM25 calls --------------------------------------------------------------------------------

    /** Whether {@code expression} is a {@code MATCH} or {@code MATCH_PHRASE} with {@code similarity_function: bm25}. */
    public static boolean isBm25(Expression expression) {
        return (expression instanceof Match || expression instanceof MatchPhrase)
            && RuntimeBm25Options.similarity(
                ((SingleFieldFullTextFunction) expression).options()
            ) == RuntimeBm25Options.RuntimeSimilarity.BM25;
    }

    /** Whether {@code expression} holds a BM25 call that is not yet wrapped with its statistics. */
    public static boolean holdsUnpreparedBm25(Expression expression) {
        return expression.anyMatch(e -> isBm25(e) && ((SingleFieldFullTextFunction) e).field() instanceof RuntimeBm25Field == false);
    }

    /** A runtime {@code MATCH} or {@code MATCH_PHRASE}, with BM25 or boolean scoring: lifted, never a narrowing condition. */
    public static boolean isRuntimeMatch(Expression expression) {
        return (expression instanceof Match || expression instanceof MatchPhrase) && ((FullTextFunction) expression).isRuntimeSearch();
    }

    /** The declared values analyzer of a BM25 call, or {@code null} for {@code standard}. */
    public static String valuesAnalyzerName(SingleFieldFullTextFunction function) {
        return function.valuesAnalyzerName();
    }

    // ---- Checks on one call --------------------------------------------------------------------------------------

    /** {@code similarity_function}, whatever its value, is a 400 on a field that is indexed in every queried source. */
    static void verifyNotIndexed(SingleFieldFullTextFunction function, Expression field, Failures failures) {
        if (RuntimeBm25Options.isSet(function.options())) {
            failures.add(
                fail(
                    function,
                    "[{}] option [similarity_function] is only supported when [{}] is a non-index-mapped field or expression; "
                        + "an indexed field is scored with the similarity declared in its mapping",
                    function.functionName(),
                    field.sourceText()
                )
            );
        }
    }

    /** With {@code bm25}: options that expand terms are a 400, and the query analyzer must be the values analyzer. */
    static void verifyBm25Options(SingleFieldFullTextFunction function, Map<String, Object> userOptions, Failures failures) {
        if (RuntimeBm25Options.similarity(function.options()) != RuntimeBm25Options.RuntimeSimilarity.BM25) {
            return;
        }
        for (String key : RuntimeBm25Options.REJECTED_WITH_BM25) {
            if (userOptions.containsKey(key)) {
                failures.add(
                    fail(function, "[{}] option [{}] is not supported with [similarity_function] = [bm25]", function.functionName(), key)
                );
            }
        }
        Object analyzer = userOptions.get("analyzer");
        String valuesAnalyzer = function.valuesAnalyzerName() == null ? "standard" : function.valuesAnalyzerName();
        if (analyzer != null && valuesAnalyzer.equals(BytesRefs.toString(analyzer)) == false) {
            failures.add(
                fail(
                    function,
                    "[{}] option [analyzer] must be [{}] with [similarity_function] = [bm25]",
                    function.functionName(),
                    valuesAnalyzer
                )
            );
        }
    }

    /**
     * Fails a scorer for a BM25 call whose field the optimizer did not wrap. The planner asked for a score that has
     * no statistics behind it; scoring it with boolean similarity would silently give the wrong ranking.
     */
    static void ensureNotBm25(SingleFieldFullTextFunction function) {
        if (isBm25(function)) {
            throw new IllegalStateException(
                "[" + function.functionName() + "] with [similarity_function] = [bm25] is scored without BM25 statistics"
            );
        }
    }

    /** The BM25 scorer of a call whose field the optimizer wrapped. Runs where the scorer is built, on the data node. */
    static ExpressionEvaluator.Factory bm25Scorer(
        SingleFieldFullTextFunction function,
        Query runtimeQuery,
        RuntimeBm25Field field,
        EvaluatorMapper.ToEvaluator toEvaluator
    ) {
        NamedAnalyzer valuesAnalyzer = RuntimeSearch.resolveNamedAnalyzer(function.valuesAnalyzerName(), toEvaluator);
        org.apache.lucene.search.Query compiled = RuntimeSearch.compileQuery(
            runtimeQuery,
            valuesAnalyzer == null ? Lucene.STANDARD_ANALYZER : valuesAnalyzer
        );
        List<ExpressionEvaluator.Factory> statistics = new ArrayList<>(field.statistics().size());
        for (Expression statistic : field.statistics()) {
            statistics.add(toEvaluator.apply(statistic));
        }
        return RuntimeSearch.bm25ScoreEvaluator(
            function.source(),
            function.functionName(),
            toEvaluator.apply(field.field()),
            field.field().sourceText(),
            compiled,
            valuesAnalyzer,
            field.termList(toEvaluator.foldCtx()),
            statistics
        );
    }

    /**
     * The sorted terms of a BM25 call, analyzed on the coordinator with the call's values analyzer. They are
     * collected from the compiled Lucene query, so synonym expansions and phrase terms are included.
     */
    public static List<BytesRef> collectTerms(SingleFieldFullTextFunction function, AnalysisRegistry registry) {
        String name = function.valuesAnalyzerName();
        NamedAnalyzer analyzer = name == null
            ? Lucene.STANDARD_ANALYZER
            : RuntimeSearch.toNamedAnalyzer(name, PlannerUtils.resolveAnalyzer(name, registry));
        Query query = switch (function) {
            case Match match -> match.runtimeQuery();
            case MatchPhrase matchPhrase -> matchPhrase.runtimeQuery();
            default -> throw new IllegalArgumentException("not a BM25 function [" + function.functionName() + "]");
        };
        try {
            return RuntimeSearch.collectTerms(RuntimeSearch.compileQuery(query, analyzer));
        } catch (IndexSearcher.TooManyClauses e) {
            throw new InvalidArgumentException(
                "the BM25 conditions on [{}] analyze to more than [{}] terms; at most [{}] are allowed",
                function.field().sourceText(),
                IndexSearcher.getMaxClauseCount(),
                IndexSearcher.getMaxClauseCount()
            );
        }
    }

    // ---- Stretches -----------------------------------------------------------------------------------------------

    enum Kind {
        /** Keeps every row: {@code WHERE}, {@code EVAL}, {@code KEEP}, {@code DROP}, {@code RENAME}, {@code SORT}. */
        TRANSPARENT,
        /** Both passes see the same rows from it: a source, {@code STATS}, {@code INLINE STATS}, a {@code _score} reader. */
        START,
        /** Decides which rows exist, or is not supported yet in Stage 1. */
        UNSUPPORTED
    }

    /** Classifies an analyzed plan node; classes not listed are unsupported. */
    static Kind classify(LogicalPlan plan) {
        return switch (plan) {
            // A filter or sort that reads _score sees the scores computed before it, so it bounds the stretch above it.
            case Filter filter -> readsScore(filter) && holdsUnpreparedBm25(filter.condition()) == false ? Kind.START : Kind.TRANSPARENT;
            case OrderBy orderBy -> readsScore(orderBy) ? Kind.START : Kind.TRANSPARENT;
            case Eval eval -> Kind.TRANSPARENT;
            // KEEP, DROP and RENAME are analyzed into Project and its subclasses, such as ResolvingProject
            case Project project -> Kind.TRANSPARENT;
            case EsRelation relation -> Kind.START;
            case ExternalRelation relation -> Kind.START;
            case Row row -> Kind.START;
            case LocalRelation relation -> Kind.START;
            case Aggregate aggregate -> Kind.START;
            case InlineStats inlineStats -> Kind.START;
            case UnionAll union -> union.children().stream().allMatch(RuntimeBm25Support::isSourceBranch) ? Kind.START : Kind.UNSUPPORTED;
            // Plan classes are an open hierarchy: anything not listed above is not supported before a BM25 call yet.
            default -> Kind.UNSUPPORTED;
        };
    }

    /** A {@code FROM} over several sources: each branch is a source, possibly under column-aligning projections. */
    private static boolean isSourceBranch(LogicalPlan branch) {
        LogicalPlan current = branch;
        while (current instanceof Project || current instanceof Eval) {
            current = ((UnaryPlan) current).child();
        }
        return current instanceof EsRelation || current instanceof ExternalRelation;
    }

    private static boolean readsScore(LogicalPlan plan) {
        return plan.expressions().stream().anyMatch(e -> e.anyMatch(MetadataAttribute::isScoreAttribute));
    }

    private static boolean isRowLimiting(LogicalPlan plan) {
        return plan instanceof Limit || plan instanceof TopN || plan instanceof Sample || plan instanceof LimitBy;
    }

    /**
     * The part of the query whose rows define the statistics of a BM25 call.
     *
     * @param start the node the stretch starts from; it is outside the stretch
     * @param top   the last node inside the stretch
     * @param nodes the nodes inside the stretch, from the bottom up
     */
    public record Stretch(LogicalPlan start, LogicalPlan top, List<LogicalPlan> nodes) {
        public List<Filter> filters() {
            return nodes.stream().filter(Filter.class::isInstance).map(Filter.class::cast).toList();
        }

        public boolean contains(LogicalPlan plan) {
            return nodes.stream().anyMatch(n -> n == plan);
        }

        /** Whether the planner will score the stretch's filters: their output carries a {@code _score} attribute. */
        public boolean scoreIsRead() {
            return top.output().stream().anyMatch(MetadataAttribute::isScoreAttribute);
        }
    }

    /** The result of walking down from a BM25 filter: a stretch, or the command that blocks one. */
    record Found(Stretch stretch, LogicalPlan blocker) {}

    /** Finds the stretch around {@code holder}, a filter holding a BM25 call. */
    static Found findStretch(LogicalPlan root, Filter holder) {
        List<LogicalPlan> below = new ArrayList<>();
        LogicalPlan current = holder;
        LogicalPlan start;
        while (true) {
            LogicalPlan child = ((UnaryPlan) current).child();
            Kind kind = classify(child);
            if (kind == Kind.START) {
                start = child;
                break;
            }
            if (kind == Kind.UNSUPPORTED) {
                return new Found(null, child);
            }
            below.add(child);
            current = child;
        }
        // A column a lifted condition reads must exist up to where the condition is re-applied. A node below the holder
        // that drops such a column bounds the stretch: the conditions under it stay where they are, in both passes.
        List<LogicalPlan> bottomUp = new ArrayList<>(below.reversed());
        bottomUp.add(holder);
        List<LogicalPlan> nodes = new ArrayList<>();
        AttributeSet required = AttributeSet.EMPTY;
        for (LogicalPlan node : bottomUp) {
            if (node.outputSet().containsAll(required) == false) {
                start = node;
                nodes.clear();
                required = AttributeSet.EMPTY;
                continue;
            }
            nodes.add(node);
            if (node instanceof Filter filter) {
                required = required.combine(searchReferences(filter));
            }
        }
        // _score must also still exist where the conditions are re-applied, so the planner scores them there.
        required = required.combine(AttributeSet.of(scoreAttributes(holder)));
        Map<LogicalPlan, LogicalPlan> parents = parents(root);
        LogicalPlan top = holder;
        for (LogicalPlan parent = parents.get(top); parent != null; parent = parents.get(parent)) {
            if (classify(parent) != Kind.TRANSPARENT || (readsScore(parent) && holdsUnpreparedBm25Filter(parent) == false)) {
                break;
            }
            if (parent instanceof Filter filter) {
                required = required.combine(searchReferences(filter));
            }
            if (parent.outputSet().containsAll(required) == false) {
                break;
            }
            nodes.add(parent);
            top = parent;
        }
        return new Found(new Stretch(start, top, nodes), null);
    }

    private static List<Attribute> scoreAttributes(LogicalPlan plan) {
        return plan.output().stream().filter(MetadataAttribute::isScoreAttribute).toList();
    }

    private static boolean holdsUnpreparedBm25Filter(LogicalPlan plan) {
        return plan instanceof Filter filter && holdsUnpreparedBm25(filter.condition());
    }

    /** The conjuncts the rule lifts out of a filter: runtime matches, and conditions reading {@code _score}. */
    public static boolean isLifted(Expression conjunct) {
        return conjunct.anyMatch(RuntimeBm25Support::isRuntimeMatch) || conjunct.anyMatch(MetadataAttribute::isScoreAttribute);
    }

    private static AttributeSet searchReferences(Filter filter) {
        AttributeSet references = AttributeSet.EMPTY;
        for (Expression conjunct : Predicates.splitAnd(filter.condition())) {
            if (isLifted(conjunct)) {
                references = references.combine(conjunct.references());
            }
        }
        return references;
    }

    private static Map<LogicalPlan, LogicalPlan> parents(LogicalPlan root) {
        Map<LogicalPlan, LogicalPlan> parents = new IdentityHashMap<>();
        root.forEachDown(plan -> {
            for (LogicalPlan child : plan.children()) {
                parents.put(child, plan);
            }
        });
        return parents;
    }

    /** The filters holding BM25 calls not yet wrapped, from the top of the plan down. */
    public static List<Filter> bm25Filters(LogicalPlan root) {
        List<Filter> filters = new ArrayList<>();
        root.forEachDown(Filter.class, filter -> {
            if (holdsUnpreparedBm25(filter.condition())) {
                filters.add(filter);
            }
        });
        return filters;
    }

    /** The stretch of the topmost BM25 filter, or {@code null} if the plan holds none or it cannot have one. */
    public static Stretch findBm25Stretch(LogicalPlan root) {
        List<Filter> filters = bm25Filters(root);
        return filters.isEmpty() ? null : findStretch(root, filters.getFirst()).stretch();
    }

    // ---- The Verifier pass ---------------------------------------------------------------------------------------

    /**
     * Adds the Stage 1 placement 400s for every BM25 call in {@code root}. It runs once over the whole analyzed plan,
     * whatever the plan holds, so the optimizer rule only sees Stage 1 shapes.
     */
    public static void placementFailures(LogicalPlan root, Failures failures) {
        root.forEachExpressionDown(Score.class, score -> score.forEachDown(Expression.class, e -> {
            if (isBm25(e)) {
                failures.add(
                    fail(
                        e,
                        "[{}] with [similarity_function] = [bm25] is not supported inside SCORE in this version; "
                            + "use it in WHERE and read _score",
                        ((FullTextFunction) e).functionName()
                    )
                );
            }
        }));

        Set<LogicalPlan> covered = Collections.newSetFromMap(new IdentityHashMap<>());
        Expression firstCall = null;
        for (Filter filter : bm25Filters(root)) {
            if (covered.contains(filter)) {
                continue;
            }
            Expression call = filter.condition().collect(RuntimeBm25Support::isBm25).getFirst();
            String function = ((FullTextFunction) call).functionName();
            Found found = findStretch(root, filter);
            if (found.blocker() != null) {
                failures.add(placementFailure(found.blocker(), function));
                continue;
            }
            if (firstCall != null) {
                failures.add(
                    fail(
                        call,
                        "[{}] with [similarity_function] = [bm25] can be used in only one part of a query in this version; "
                            + "[{}] already uses it",
                        function,
                        firstCall.sourceText()
                    )
                );
                continue;
            }
            firstCall = call;
            covered.addAll(found.stretch().nodes());
            otherFullTextFailures(found.stretch(), function, failures);
            skipRowFailures(found.stretch().start(), function, failures);
        }
    }

    private static Failure placementFailure(LogicalPlan blocker, String function) {
        String command = blocker.sourceText();
        if (isRowLimiting(blocker)) {
            return fail(
                blocker,
                "[{}] with [similarity_function] = [bm25] cannot compute statistics after [{}], which decides which rows exist "
                    + "before it; move [{}] after the WHERE, or set [similarity_function] to [boolean]",
                function,
                command,
                command
            );
        }
        return fail(
            blocker,
            "[{}] with [similarity_function] = [bm25] cannot compute statistics after [{}] in this version; "
                + "set [similarity_function] to [boolean]",
            function,
            command
        );
    }

    /** Full-text functions that need Lucene push-down cannot share a stretch with a BM25 call in Stage 1. */
    private static void otherFullTextFailures(Stretch stretch, String function, Failures failures) {
        for (LogicalPlan node : stretch.nodes()) {
            for (Expression expression : node.expressions()) {
                expression.forEachDown(FullTextFunction.class, other -> {
                    // BM25 calls report their own errors, such as an indexed field.
                    if (isRuntimeMatch(other) == false && isBm25(other) == false) {
                        failures.add(
                            fail(
                                other,
                                "[{}] cannot be used in the same part of the query as [{}] with [similarity_function] = [bm25]; "
                                    + "set [similarity_function] to [boolean]",
                                other.sourceText(),
                                function
                            )
                        );
                    }
                });
            }
        }
    }

    /** A dataset read with {@code error_mode: skip_row} can drop different rows in each pass, which read different columns. */
    private static void skipRowFailures(LogicalPlan start, String function, Failures failures) {
        start.forEachDown(ExternalRelation.class, relation -> {
            Map<String, Object> config = relation.metadata() == null ? null : relation.metadata().config();
            if (config != null && ErrorPolicy.forReader(config, null).mode() == ErrorPolicy.Mode.SKIP_ROW) {
                String dataset = relation.datasetName() != null ? relation.datasetName() : relation.sourcePath();
                failures.add(
                    fail(
                        relation,
                        "[{}] with [similarity_function] = [bm25] is not supported on dataset [{}], which is read with "
                            + "[error_mode] = [skip_row]",
                        function,
                        dataset
                    )
                );
            }
        });
    }

    // ---- The post-optimization check -----------------------------------------------------------------------------

    /** Whether {@code join} is a BM25 statistics join: its right side evaluates {@link RuntimeBm25RowStats}. */
    public static boolean isStatisticsJoin(InlineJoin join) {
        return join.right().anyMatch(p -> p.expressions().stream().anyMatch(e -> e.anyMatch(RuntimeBm25RowStats.class::isInstance)));
    }

    /**
     * Throws if a BM25 call ended up on the left side of its own statistics join. The rule re-applies every BM25 call
     * above the join, with its field wrapped in its statistics; one below the join would narrow the rows its own
     * statistics count. It is a bug, not a user error, so it fails with a server error.
     */
    public static void checkStatisticsJoins(LogicalPlan optimized) {
        optimized.forEachDown(InlineJoin.class, join -> {
            if (isStatisticsJoin(join) == false) {
                return;
            }
            join.left().forEachDown(Filter.class, filter -> {
                if (filter.condition().anyMatch(RuntimeBm25Field.class::isInstance)) {
                    throw new IllegalStateException("a BM25 condition sits below its statistics join: " + filter);
                }
            });
        });
    }
}
