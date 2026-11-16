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
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryParser;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.ParsedEsqlQuery;

import java.util.ArrayList;
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

    private final EsqlParser parser = new EsqlParser(new EsqlConfig(new EsqlFunctionRegistry()));
    private final PreAnalyzer preAnalyzer = new PreAnalyzer();

    @Override
    public ParsedEsqlQuery parse(String query) {
        EsqlStatement statement = parser.parse(
            query,
            new QueryParams(),
            new InferenceSettings(Settings.EMPTY),
            QueryPragmas.EMPTY,
            EsqlFlags.DEFAULTS
        );
        LogicalPlan plan = statement.plan();
        TopLevelPipeline pipeline = TopLevelPipeline.of(plan);
        Set<String> indexPatterns = new TreeSet<>();
        preAnalyzer.preAnalyze(plan)
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
        private final List<int[]> limitsWithAggregationsAbove = new ArrayList<>();
        private final Set<String> filterFields = new LinkedHashSet<>();
        private final Set<String> sortFields = new LinkedHashSet<>();
        private LogicalPlan source;
        private int aggregates;

        static TopLevelPipeline of(LogicalPlan plan) {
            TopLevelPipeline pipeline = new TopLevelPipeline();
            boolean topLevel = true;
            LogicalPlan node = plan;
            // FROM with subqueries is the source command and FORK ends the top-level pipeline; both lead on through the first child.
            while (node instanceof UnionAll == false && node.children().isEmpty() == false) {
                if (node instanceof MergePlan) {
                    topLevel = false;
                } else if (topLevel) {
                    pipeline.visit(node);
                }
                node = node instanceof InlineStats inline && inline.child() instanceof Aggregate aggregate
                    ? aggregate.child()
                    : node.children().getFirst();
            }
            pipeline.source = node;
            return pipeline;
        }

        private void visit(LogicalPlan node) {
            switch (node) {
                case Limit limit -> limitsWithAggregationsAbove.add(new int[] { limitValue(limit), aggregates });
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
            return minLimit(0);
        }

        /** Limits before the first STATS of the query. */
        OptionalInt limitBeforeAggregation() {
            return aggregates == 0 ? OptionalInt.empty() : minLimit(aggregates);
        }

        private OptionalInt minLimit(int aggregatesAbove) {
            return limitsWithAggregationsAbove.stream().filter(limit -> limit[1] == aggregatesAbove).mapToInt(limit -> limit[0]).min();
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
