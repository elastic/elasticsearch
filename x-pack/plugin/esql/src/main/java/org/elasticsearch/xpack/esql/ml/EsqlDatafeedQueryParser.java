/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ml;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.MultiColumnInSubquery;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.QuerySetting;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.ResolvedSettings;
import org.elasticsearch.xpack.esql.plan.SettingsValidationContext;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryParser;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryWithParams;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.ParsedEsqlQuery;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.TimeBucket;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam.ParamClassification.IDENTIFIER;

/**
 * {@link EsqlQueryParser} for ES|QL datafeeds, backed by {@link EsqlParser}. It parses only: nothing is resolved against mappings.
 * Settings and limits are resolved against the cluster the way {@code EsqlSession} does for a query it runs.
 */
public class EsqlDatafeedQueryParser implements EsqlQueryParser {

    /** Built on first use, so loading this provider at node startup does not build a function registry. */
    private static final class Holder {
        static final EsqlParser PARSER = new EsqlParser(new EsqlConfig(new EsqlFunctionRegistry()));
        static final PreAnalyzer PRE_ANALYZER = new PreAnalyzer();
    }

    @Override
    public ParsedEsqlQuery parse(String query, ClusterService clusterService) {
        Settings nodeSettings = clusterService.getSettings();
        ClusterState clusterState = clusterService.state();
        // Dynamic cluster settings override node configuration, as they do for queries ES|QL runs.
        Settings effectiveSettings = Settings.builder().put(nodeSettings).put(clusterState.metadata().settings()).build();
        EsqlStatement statement = Holder.PARSER.parse(
            query,
            new QueryParams(),
            new InferenceSettings(effectiveSettings),
            QueryPragmas.EMPTY,
            new EsqlFlags(clusterService.getClusterSettings())
        );
        SettingsValidationContext validation = SettingsValidationContext.from(new CrossProjectModeDecider(nodeSettings));
        QuerySettings.validate(statement, validation);
        ResolvedSettings settings = QuerySettings.resolve(
            clusterState.metadata().settings(),
            nodeSettings,
            Map.of(),
            statement,
            validation
        );
        // The probe sets column_metadata itself, and the last SET of a setting wins.
        if (statement.settings().stream().anyMatch(setting -> QuerySettings.COLUMN_METADATA.name().equals(setting.name()))) {
            throw new ParsingException(
                statement.plan().source(),
                "[SET {}] is not supported in ES|QL datafeeds",
                QuerySettings.COLUMN_METADATA.name()
            );
        }
        if (statement.letBindings().isEmpty() == false) {
            throw unsupported("LET", statement.plan());
        }
        statement.plan().forEachDown(node -> {
            String command = unsupportedCommand(node);
            if (command != null) {
                throw unsupported(command, node);
            }
            // The relations inside an IN subquery would be missed by the single-pipeline reading.
            node.forEachExpression(InSubquery.class, in -> { throw unsupported("IN (subquery)", node); });
            node.forEachExpression(MultiColumnInSubquery.class, in -> { throw unsupported("IN (subquery)", node); });
        });
        LogicalPlan plan = statement.plan();
        Pipeline pipeline = Pipeline.of(plan);
        Set<String> indexPatterns = new TreeSet<>();
        Holder.PRE_ANALYZER.preAnalyze(plan)
            .indexes()
            .keySet()
            .forEach(pattern -> indexPatterns.addAll(List.of(Strings.splitStringByCommaToArray(pattern.indexPattern()))));
        return new Parsed(
            query,
            statement.settings().stream().map(setting -> setting.source().text()).collect(Collectors.joining("\n")),
            pipeline.source instanceof UnresolvedRelation relation ? sourceCommand(relation) : "",
            indexPatterns,
            pipeline.aggregates > 0,
            pipeline.outputLimit(),
            pipeline.filterFields,
            statement.settings().stream().map(QuerySetting::name).anyMatch(QuerySettings.PROJECT_ROUTING.name()::equals),
            QuerySettings.TIME_ZONE.get(settings)
        );
    }

    /** The command that makes a query unsupported, or {@code null}. These are the constructs whose result columns cannot be traced. */
    private static String unsupportedCommand(LogicalPlan node) {
        if (node instanceof Fork) {
            return "FORK";
        }
        if (node instanceof UnionAll) {
            return "FROM with several subqueries";
        }
        if (node instanceof Rename) {
            return "RENAME";
        }
        if (node instanceof MvExpand) {
            return "MV_EXPAND";
        }
        if (node instanceof LookupJoin) {
            return "LOOKUP JOIN";
        }
        if (node instanceof PromqlCommand) {
            return "PROMQL";
        }
        return null;
    }

    /**
     * Source probes aggregate with {@code COUNT(*)}, which {@code TS} rejects, so a {@code TS} source is read with {@code FROM}.
     * {@code TS} only reads time series indices, so the {@code FROM} keeps that scope with an {@code _index_mode} filter.
     */
    private static String sourceCommand(UnresolvedRelation relation) {
        String text = relation.source().text();
        if (relation.isTimeSeriesMode() == false) {
            return text;
        }
        String from = "FROM" + text.substring(2);
        if (relation.metadataFields().isEmpty()) {
            from += " METADATA _index_mode";
        } else if (relation.metadataFields().stream().noneMatch(field -> field.name().equals("_index_mode"))) {
            from += ", _index_mode";
        }
        return from + " | WHERE _index_mode == \"time_series\"";
    }

    private static ParsingException unsupported(String command, LogicalPlan node) {
        return new ParsingException(node.source(), "[{}] is not supported in ES|QL datafeeds", command);
    }

    /** What datafeeds ask of the commands, collected in one walk from the last command to the source. */
    private static final class Pipeline {
        private record LimitPosition(Limit limit, int value, int aggregatesAfter) {}

        private final List<LimitPosition> limits = new ArrayList<>();
        private final Set<String> filterFields = new LinkedHashSet<>();
        private LogicalPlan source;
        private int aggregates;

        static Pipeline of(LogicalPlan plan) {
            Pipeline pipeline = new Pipeline();
            LogicalPlan node = plan;
            while (node.children().isEmpty() == false) {
                if (node.children().size() > 1) {
                    throw unsupported(node.nodeName(), node);
                }
                pipeline.visit(node);
                // INLINE STATS keeps every input row, so its Aggregate does not aggregate the pipeline.
                node = node instanceof InlineStats inline && inline.child() instanceof Aggregate aggregate
                    ? aggregate.child()
                    : node.children().getFirst();
            }
            pipeline.source = node;
            pipeline.rejectLimitBeforeAggregation();
            return pipeline;
        }

        private void visit(LogicalPlan node) {
            if (node instanceof Limit limit) {
                limits.add(new LimitPosition(limit, limitValue(limit), aggregates));
            } else if (node instanceof Aggregate) {
                aggregates++;
            } else if (node instanceof Filter filter) {
                filter.forEachExpression(UnresolvedAttribute.class, attribute -> filterFields.add(attribute.name()));
            }
        }

        private static int limitValue(Limit limit) {
            Expression value = limit.limit();
            return value instanceof Literal literal && literal.value() instanceof Number number ? number.intValue() : Integer.MAX_VALUE;
        }

        /** A LIMIT below every STATS feeds an arbitrary subset of rows into the aggregation, in each window. */
        private void rejectLimitBeforeAggregation() {
            for (LimitPosition position : limits) {
                if (aggregates > 0 && position.aggregatesAfter == aggregates) {
                    throw new ParsingException(
                        position.limit.source(),
                        "[LIMIT] before [STATS] is not supported in ES|QL datafeeds: it feeds an arbitrary subset of rows into the "
                            + "aggregation. Move the LIMIT after the STATS or remove it"
                    );
                }
            }
        }

        /** Limits after the last STATS of the query, or all limits when there is no STATS. */
        OptionalInt outputLimit() {
            return limits.stream().filter(limit -> limit.aggregatesAfter() == 0).mapToInt(LimitPosition::value).min();
        }
    }

    private record Parsed(
        String query,
        String settingsText,
        String sourceCommand,
        Set<String> indexPatterns,
        boolean isAggregating,
        OptionalInt outputLimit,
        Set<String> filterFields,
        boolean setsProjectRoutingInQuery,
        ZoneId timeZone
    ) implements ParsedEsqlQuery {

        private static final String TIME_FIELD = "timeField";

        Parsed {
            indexPatterns = Collections.unmodifiableSet(indexPatterns);
            filterFields = Collections.unmodifiableSet(filterFields);
        }

        @Override
        public boolean filtersOn(String field) {
            return filterFields.contains(field);
        }

        @Override
        public EsqlQueryWithParams limitZeroQuery() {
            return withPipeline("LIMIT 0", List.of());
        }

        @Override
        public EsqlQueryWithParams orderedByTimeQuery(String timeField, OptionalLong rowLimit) {
            String pipeline = rowLimit.isPresent() ? "SORT ??timeField ASC\n| LIMIT " + rowLimit.getAsLong() : "SORT ??timeField ASC";
            return withPipeline(pipeline, timeFieldParams(timeField));
        }

        @Override
        public EsqlQueryWithParams columnMetadataProbeQuery() {
            return new EsqlQueryWithParams("SET column_metadata=true;\n" + query, List.of());
        }

        @Override
        public Optional<TimeBucket> timeBucket(List<? extends ColumnInfo> columns, String timeColumn) {
            for (ColumnInfo column : columns) {
                if (column instanceof ColumnInfoImpl info
                    && info.name().equals(timeColumn)
                    && info.meta() != null
                    && info.meta().get("bucket") instanceof Map<?, ?> bucket
                    && bucket.get("interval") instanceof Number interval
                    && bucket.get("unit") instanceof String unit) {
                    return Optional.of(new TimeBucket(interval.longValue(), unit));
                }
            }
            return Optional.empty();
        }

        @Override
        public EsqlQueryWithParams outputRowCountQuery() {
            return withPipeline("STATS " + OUTPUT_ROWS_COLUMN + " = COUNT(*)", List.of());
        }

        @Override
        public EsqlQueryWithParams sourceRangeSummaryQuery(String sourceTimeField) {
            return sourceWithPipeline(
                "STATS "
                    + EARLIEST_TIME_COLUMN
                    + " = MIN(??timeField), "
                    + LATEST_TIME_COLUMN
                    + " = MAX(??timeField), "
                    + TOTAL_HITS_COLUMN
                    + " = COUNT(*)",
                timeFieldParams(sourceTimeField)
            );
        }

        @Override
        public EsqlQueryWithParams sourceTimeFieldProbeQuery(String sourceTimeField) {
            return sourceWithPipeline("KEEP ??timeField | LIMIT 0", timeFieldParams(sourceTimeField));
        }

        private EsqlQueryWithParams withPipeline(String pipeline, List<EsqlQueryParam> params) {
            // A new line keeps a trailing // comment of the query from swallowing the pipeline.
            return new EsqlQueryWithParams(query + "\n| " + pipeline, params);
        }

        /** The {@code SET} statements and source command, then the pipeline; the user's own commands are left out. */
        private EsqlQueryWithParams sourceWithPipeline(String pipeline, List<EsqlQueryParam> params) {
            if (sourceCommand.isEmpty()) {
                throw new IllegalStateException("the query does not read indices");
            }
            String source = settingsText.isEmpty() ? sourceCommand : settingsText + "\n" + sourceCommand;
            return new EsqlQueryWithParams(source + " | " + pipeline, params);
        }

        private static List<EsqlQueryParam> timeFieldParams(String field) {
            return List.of(new EsqlQueryParam(TIME_FIELD, field, IDENTIFIER));
        }
    }
}
