/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ml;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.QuerySetting;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryParser;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.ParsedEsqlQuery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/** {@link EsqlQueryParser} for ES|QL datafeeds, backed by {@link EsqlParser}. It parses only: nothing is resolved against mappings. */
public class EsqlDatafeedQueryParser implements EsqlQueryParser {

    /** Built on first use, so loading this provider at node startup does not build a function registry. */
    private static final class Holder {
        static final EsqlParser PARSER = new EsqlParser(new EsqlConfig(new EsqlFunctionRegistry()));
        static final PreAnalyzer PRE_ANALYZER = new PreAnalyzer();
    }

    @Override
    public ParsedEsqlQuery parse(String query) {
        EsqlStatement statement = Holder.PARSER.parse(
            query,
            new QueryParams(),
            new InferenceSettings(Settings.EMPTY),
            QueryPragmas.EMPTY,
            EsqlFlags.DEFAULTS
        );
        LogicalPlan plan = statement.plan();
        TopLevelPipeline pipeline = TopLevelPipeline.of(plan);
        Set<String> indexPatterns = new TreeSet<>();
        Holder.PRE_ANALYZER.preAnalyze(plan)
            .indexes()
            .keySet()
            .forEach(pattern -> indexPatterns.addAll(List.of(Strings.splitStringByCommaToArray(pattern.indexPattern()))));
        return new Parsed(
            query,
            settings(statement),
            statement.settings().stream().map(setting -> setting.source().text()).collect(Collectors.joining("\n")),
            sourceCommand(pipeline.source),
            pipeline.source instanceof UnresolvedRelation || pipeline.source instanceof UnionAll,
            indexPatterns,
            pipeline.aggregates > 0,
            pipeline.outputLimit(),
            pipeline.limitBeforeAggregation(),
            pipeline.filterFields,
            pipeline.sortFields
        );
    }

    private static Map<String, String> settings(EsqlStatement statement) {
        Map<String, String> settings = new LinkedHashMap<>();
        for (QuerySetting setting : statement.settings()) {
            Expression value = setting.value();
            settings.put(setting.name(), value instanceof Literal literal ? BytesRefs.toString(literal.value()) : value.sourceText());
        }
        return settings;
    }

    private static String sourceCommand(LogicalPlan source) {
        if (source instanceof UnresolvedRelation) {
            return source.source().text();
        } else if (source instanceof UnionAll) {
            // The UnionAll source starts after the FROM keyword and ends before METADATA.
            return "FROM " + source.source().text();
        }
        return "";
    }

    /** What datafeeds ask of the commands after the source, collected in one walk from the last command to the source. */
    private static final class TopLevelPipeline {
        private record LimitPosition(int value, int aggregatesAbove, boolean rowsExpandedAbove) {}

        private final List<LimitPosition> limits = new ArrayList<>();
        private final Set<String> filterFields = new LinkedHashSet<>();
        private final Set<String> sortFields = new LinkedHashSet<>();
        private LogicalPlan source;
        private int aggregates;
        private boolean rowsExpanded;

        static TopLevelPipeline of(LogicalPlan plan) {
            TopLevelPipeline pipeline = new TopLevelPipeline();
            LogicalPlan node = plan;
            // FROM with subqueries is the source command and leads on through the first child. FORK branches are skipped: the walk
            // continues at the input all branches share.
            while (node instanceof UnionAll == false && node.children().isEmpty() == false) {
                if (node instanceof Fork fork) {
                    pipeline.rowsExpanded = true;
                    node = sharedInput(fork);
                    continue;
                }
                pipeline.visit(node);
                node = node instanceof InlineStats inline && inline.child() instanceof Aggregate aggregate
                    ? aggregate.child()
                    : node.children().getFirst();
            }
            pipeline.source = node;
            return pipeline;
        }

        /** The plan all branches of {@code fork} read, or the source of the first branch when it cannot be found (one branch). */
        private static LogicalPlan sharedInput(Fork fork) {
            List<LogicalPlan> branches = fork.children();
            Set<LogicalPlan> second = Collections.newSetFromMap(new IdentityHashMap<>());
            if (branches.size() > 1) {
                for (LogicalPlan n = branches.get(1); n != null; n = n.children().isEmpty() ? null : n.children().getFirst()) {
                    second.add(n);
                }
            }
            LogicalPlan n = branches.getFirst();
            while (second.contains(n) == false && n.children().isEmpty() == false) {
                n = n.children().getFirst();
            }
            return n;
        }

        private void visit(LogicalPlan node) {
            switch (node) {
                case Limit limit -> limits.add(new LimitPosition(limitValue(limit), aggregates, rowsExpanded));
                case MvExpand ignored -> rowsExpanded = true;
                case LookupJoin ignored -> rowsExpanded = true;
                case Aggregate ignored -> aggregates++;
                case Filter filter -> addFields(filter, filterFields);
                case OrderBy orderBy -> addFields(orderBy, sortFields);
                default -> {
                }
            }
        }

        private static int limitValue(Limit limit) {
            return limit.limit() instanceof Literal literal && literal.value() instanceof Number number
                ? number.intValue()
                : Integer.MAX_VALUE;
        }

        private static void addFields(LogicalPlan node, Set<String> fields) {
            node.forEachExpression(UnresolvedAttribute.class, attribute -> fields.add(attribute.name()));
        }

        /** Limits after the last STATS of the query, or all limits when there is no STATS. */
        OptionalInt outputLimit() {
            return minLimit(0, false);
        }

        /** Limits before the first STATS of the query. */
        OptionalInt limitBeforeAggregation() {
            return aggregates == 0 ? OptionalInt.empty() : minLimit(aggregates, true);
        }

        private OptionalInt minLimit(int aggregatesAbove, boolean ignoreRowExpanders) {
            return limits.stream()
                .filter(limit -> limit.aggregatesAbove() == aggregatesAbove && (ignoreRowExpanders || limit.rowsExpandedAbove() == false))
                .mapToInt(LimitPosition::value)
                .min();
        }
    }

    private record Parsed(
        String query,
        Map<String, String> settings,
        String settingsText,
        String sourceCommand,
        boolean readsIndices,
        Set<String> indexPatterns,
        boolean aggregates,
        OptionalInt outputLimit,
        OptionalInt limitBeforeAggregation,
        Set<String> filterFields,
        Set<String> sortFields
    ) implements ParsedEsqlQuery {

        Parsed {
            settings = Collections.unmodifiableMap(settings);
            indexPatterns = Collections.unmodifiableSet(indexPatterns);
            filterFields = Collections.unmodifiableSet(filterFields);
            sortFields = Collections.unmodifiableSet(sortFields);
        }

        @Override
        public Optional<String> setting(String name) {
            return Optional.ofNullable(settings.get(name));
        }

        @Override
        public boolean filtersOn(String field) {
            return filterFields.contains(field);
        }

        @Override
        public boolean sortsOn(String field) {
            return sortFields.contains(field);
        }

        @Override
        public String withPipeline(String pipeline) {
            // A new line keeps a trailing // comment of the query from swallowing the pipeline.
            return query + "\n| " + pipeline;
        }

        @Override
        public String sourceWithPipeline(String pipeline) {
            if (readsIndices == false) {
                throw new IllegalStateException("the query does not read indices");
            }
            String source = settingsText.isEmpty() ? sourceCommand : settingsText + "\n" + sourceCommand;
            return source + "\n| " + pipeline;
        }
    }
}
