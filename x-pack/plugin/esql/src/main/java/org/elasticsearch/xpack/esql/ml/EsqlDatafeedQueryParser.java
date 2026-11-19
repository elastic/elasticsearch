/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ml;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.MultiColumnInSubquery;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.QuerySetting;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.SettingsValidationContext;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.ChangePoint;
import org.elasticsearch.xpack.esql.plan.logical.Dedup;
import org.elasticsearch.xpack.esql.plan.logical.Describe;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Explain;
import org.elasticsearch.xpack.esql.plan.logical.FillNull;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.Highlight;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Into;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Lookup;
import org.elasticsearch.xpack.esql.plan.logical.MMR;
import org.elasticsearch.xpack.esql.plan.logical.MetricsInfo;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.NestedExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.RegisteredDomain;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesCollapse;
import org.elasticsearch.xpack.esql.plan.logical.TsInfo;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedEqlRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedIpLocation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedMetadata;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.UriParts;
import org.elasticsearch.xpack.esql.plan.logical.UserAgent;
import org.elasticsearch.xpack.esql.plan.logical.fuse.Fuse;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.inference.DenseVector;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.show.ShowInfo;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.view.ViewService;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryParser;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryWithParams;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.ParsedEsqlQuery;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.TimeBucket;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam.ParamClassification.IDENTIFIER;

/**
 * {@link EsqlQueryParser} for ES|QL datafeeds, backed by {@link EsqlParser}. It parses only: nothing is resolved against mappings.
 * Parsing and settings resolution are the steps {@code EsqlSession} runs first for a query ({@link EsqlQueryRequest#parse} and
 * {@code QuerySettings#resolve}); the parser uses the services of the {@link EsqlPlugin} that provides it.
 */
public class EsqlDatafeedQueryParser implements EsqlQueryParser {

    /** What the parser needs from the {@link EsqlPlugin}; they exist only after the plugin created its components. */
    public record Services(
        EsqlParser parser,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        ViewService viewService,
        CrossProjectModeDecider crossProjectModeDecider,
        InferenceService inferenceService,
        XPackLicenseState licenseState
    ) {}

    /**
     * Commands that give the same rows for a time window whichever other rows are in it, so they may be anywhere in the query. A
     * datafeed runs the query once per chunk of its time window, and the chunk can be made smaller and retried; a command whose
     * output depends on the other rows of the window would give different results for each chunking.
     */
    static final Set<Class<? extends LogicalPlan>> ANYWHERE = Set.of(
        UnresolvedRelation.class,
        Row.class,
        UnresolvedMetadata.class,
        Subquery.class,
        Filter.class,
        OrderBy.class,
        Eval.class,
        Keep.class,
        Drop.class,
        Dissect.class,
        Grok.class,
        Enrich.class,
        UriParts.class,
        RegisteredDomain.class,
        UserAgent.class,
        UnresolvedIpLocation.class,
        Highlight.class,
        FillNull.class,
        Aggregate.class,
        TimeSeriesAggregate.class,
        InlineStats.class
    );

    /**
     * Commands whose output depends on the other rows of the time window, with their names. Only {@code LIMIT} is accepted: after the
     * last {@code STATS} (or {@code INLINE STATS}), where it shapes the output, and in a query without {@code STATS}.
     */
    static final Map<Class<? extends LogicalPlan>, String> AFTER_LAST_STATS = Map.of(Limit.class, "LIMIT");

    /**
     * Commands datafeeds do not support, with their names: the ones whose result columns cannot be traced to the source, the ones
     * that do not read index rows, the ones that call an inference endpoint for every row, and the ones whose result depends on how
     * the time window is chunked or sampled ({@code SAMPLE} gives counts the delayed data check cannot match).
     */
    static final Map<Class<? extends LogicalPlan>, String> REJECTED = Map.ofEntries(
        Map.entry(Fork.class, "FORK"),
        Map.entry(LimitBy.class, "LIMIT BY"),
        Map.entry(Dedup.class, "DEDUP"),
        Map.entry(ChangePoint.class, "CHANGE_POINT"),
        Map.entry(Fuse.class, "FUSE"),
        Map.entry(MMR.class, "MMR"),
        Map.entry(Sample.class, "SAMPLE"),
        Map.entry(UnionAll.class, "FROM with several subqueries"),
        Map.entry(Rename.class, "RENAME"),
        Map.entry(MvExpand.class, "MV_EXPAND"),
        Map.entry(NestedExpand.class, "NESTED_EXPAND"),
        Map.entry(LookupJoin.class, "LOOKUP JOIN"),
        Map.entry(Lookup.class, "LOOKUP"),
        Map.entry(PromqlCommand.class, "PROMQL"),
        Map.entry(Explain.class, "EXPLAIN"),
        Map.entry(Into.class, "INTO"),
        Map.entry(Describe.class, "DESCRIBE"),
        Map.entry(ShowInfo.class, "SHOW"),
        Map.entry(MetricsInfo.class, "METRICS_INFO"),
        Map.entry(TsInfo.class, "TS_INFO"),
        Map.entry(TimeSeriesCollapse.class, "TS_COLLAPSE"),
        Map.entry(UnresolvedExternalRelation.class, "EXTERNAL"),
        Map.entry(UnresolvedEqlRelation.class, "EQL"),
        Map.entry(Completion.class, "COMPLETION"),
        Map.entry(Rerank.class, "RERANK"),
        Map.entry(DenseVector.class, "DENSE_VECTOR")
    );

    private final Supplier<Services> services;
    private final PreAnalyzer preAnalyzer = new PreAnalyzer();

    /** Required by the service loader, which constructs the provider with the plugin. */
    public EsqlDatafeedQueryParser() {
        throw new IllegalStateException("Provider must be constructed using PluginsService");
    }

    public EsqlDatafeedQueryParser(EsqlPlugin plugin) {
        this(plugin::datafeedParserServices);
    }

    EsqlDatafeedQueryParser(Supplier<Services> services) {
        this.services = services;
    }

    /**
     * For tests of the callers of the parser, which build the services without an {@link EsqlPlugin}. It is a factory and not a
     * constructor because the extension loader needs the provider to have a single public constructor.
     */
    public static EsqlDatafeedQueryParser withServices(Supplier<Services> services) {
        return new EsqlDatafeedQueryParser(services);
    }

    /**
     * Tech Preview only: it re-implements a subset of the analysis pipeline (plan walk).
     * It is to be replaced by the primitives behind {@code DESCRIBE}, or by a call to
     * {@code DESCRIBE} and handling of its response. Tracked in
     * https://github.com/elastic/elasticsearch/issues/166310
     */
    @Override
    public ParsedEsqlQuery parse(String query) {
        Services services = this.services.get();
        ClusterService clusterService = services.clusterService();
        ClusterState clusterState = clusterService.state();
        SettingsValidationContext validation = SettingsValidationContext.from(services.crossProjectModeDecider());
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest(query);
        EsqlStatement statement = request.parse(
            services.parser(),
            validation,
            // The instance queries use, kept current by its settings-update consumer.
            services.inferenceService().inferenceSettings(),
            new EsqlFlags(clusterService.getClusterSettings())
        );
        // The same call as EsqlSession#doExecute, so a setting a query could not run with is rejected as it would be there.
        QuerySettings.resolve(
            clusterService.state().metadata().settings(),
            clusterService.getSettings(),
            request.requestSettings(),
            statement,
            validation,
            services.licenseState()
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
            String command = REJECTED.get(node.getClass());
            if (command != null) {
                throw unsupported(command, node);
            }
            // A command nobody classified fails here rather than running with an unknown effect on the rows.
            if (ANYWHERE.contains(node.getClass()) == false && AFTER_LAST_STATS.containsKey(node.getClass()) == false) {
                throw unsupported(node.nodeName(), node);
            }
            // The relations inside an IN subquery would be missed by the single-pipeline reading.
            node.forEachExpression(InSubquery.class, in -> { throw unsupported("IN (subquery)", node); });
            node.forEachExpression(MultiColumnInSubquery.class, in -> { throw unsupported("IN (subquery)", node); });
        });
        LogicalPlan plan = statement.plan();
        Pipeline pipeline = Pipeline.of(plan);
        Set<String> indexPatterns = new TreeSet<>();
        preAnalyzer.preAnalyze(plan)
            .indexes()
            .keySet()
            .forEach(pattern -> indexPatterns.addAll(List.of(Strings.splitStringByCommaToArray(pattern.indexPattern()))));
        rejectViews(indexPatterns, services, clusterState);
        return new Parsed(
            query,
            statement.settings().stream().map(setting -> setting.source().text()).collect(Collectors.joining("\n")),
            pipeline.source instanceof UnresolvedRelation relation ? relation.source().text() : "",
            pipeline.source instanceof UnresolvedRelation relation && relation.isTimeSeriesMode(),
            indexPatterns,
            pipeline.aggregates > 0,
            pipeline.outputLimit(),
            statement.settings().stream().map(QuerySetting::name).anyMatch(QuerySettings.PROJECT_ROUTING.name()::equals),
            // ML runs every query in UTC whatever the cluster default is, so only the query's own SET is of interest.
            QuerySettings.TIME_ZONE.get(QuerySettings.resolve(Map.of(), statement, validation))
        );
    }

    /**
     * A view is a query of its own, so its output rows cannot be traced to the source the datafeed reads. Only a view named exactly
     * is found: a wildcard, a remote or an expression that resolves to a view is not, which is a Tech Preview limitation.
     */
    private static void rejectViews(Set<String> indexPatterns, Services services, ClusterState clusterState) {
        ProjectId projectId = services.projectResolver().getProjectMetadata(clusterState).id();
        for (String pattern : indexPatterns) {
            boolean exactName = pattern.contains("*") == false
                && pattern.contains(":") == false
                && pattern.startsWith("-") == false
                && pattern.startsWith("<") == false;
            if (exactName && services.viewService().get(projectId, pattern) != null) {
                throw new ParsingException(Source.EMPTY, "[{}] is a view; views are not supported in ES|QL datafeeds", pattern);
            }
        }
    }

    private static ParsingException unsupported(String command, LogicalPlan node) {
        return new ParsingException(node.source(), "[{}] is not supported in ES|QL datafeeds", command);
    }

    /** What datafeeds ask of the commands, collected in one walk from the last command to the source. */
    private static final class Pipeline {
        private final List<Integer> limits = new ArrayList<>();
        private LogicalPlan source;
        private int aggregates;
        /** The {@code STATS} and {@code INLINE STATS} commands after the node the walk is at; they make it a feed of an aggregation. */
        private int boundaries;
        private String lastBoundary = "STATS";

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
            return pipeline;
        }

        private void visit(LogicalPlan node) {
            String command = AFTER_LAST_STATS.get(node.getClass());
            if (command != null && boundaries > 0) {
                throw new ParsingException(
                    node.source(),
                    "[{}] before [{}] is not supported in ES|QL datafeeds: it feeds rows that depend on the other rows of the time "
                        + "window into the aggregation. Move it after the last {} or remove it",
                    command,
                    lastBoundary,
                    lastBoundary
                );
            }
            if (node instanceof Limit limit) {
                limits.add(limitValue(limit));
            } else if (node instanceof Aggregate) {
                aggregates++;
                boundaries++;
                lastBoundary = "STATS";
            } else if (node instanceof InlineStats) {
                boundaries++;
                lastBoundary = "INLINE STATS";
            }
        }

        private static int limitValue(Limit limit) {
            if (limit.limit() instanceof Literal literal && literal.value() instanceof Number number) {
                return number.intValue();
            }
            throw new IllegalStateException("the parser keeps the value of a LIMIT as a literal: " + limit.limit());
        }

        /** Limits after the last STATS of the query, or all limits when there is no STATS; the others are rejected. */
        OptionalInt outputLimit() {
            return limits.stream().mapToInt(Integer::intValue).min();
        }
    }

    private record Parsed(
        String query,
        String settingsText,
        String sourceCommand,
        boolean timeSeriesSource,
        Set<String> indexPatterns,
        boolean isAggregating,
        OptionalInt outputLimit,
        boolean setsProjectRoutingInQuery,
        ZoneId timeZone
    ) implements ParsedEsqlQuery {

        private static final String TIME_FIELD = "timeField";

        Parsed {
            indexPatterns = Collections.unmodifiableSet(indexPatterns);
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
            if (timeSeriesSource && isAggregating == false) {
                // TS rejects COUNT(*) as its first aggregation, so the generated query could not run.
                throw new IllegalStateException("cannot count the output rows of a TS query without STATS");
            }
            return withPipeline("STATS " + OUTPUT_ROWS_COLUMN + " = COUNT(*)", List.of());
        }

        /**
         * {@code TS} rejects {@code COUNT(*)} and {@code MIN(field)}: it aggregates over time, so a {@code TS} source is read with
         * the over-time functions, which also keeps it to the time series indices that {@code TS} reads. A document without the time
         * field is not counted then, which does not matter for a range summary.
         */
        @Override
        public EsqlQueryWithParams sourceRangeSummaryQuery(String sourceTimeField) {
            String stats = timeSeriesSource
                ? "STATS "
                    + EARLIEST_TIME_COLUMN
                    + " = MIN(MIN_OVER_TIME(??timeField)), "
                    + LATEST_TIME_COLUMN
                    + " = MAX(MAX_OVER_TIME(??timeField)), "
                    + TOTAL_HITS_COLUMN
                    + " = COALESCE(SUM(COUNT_OVER_TIME(??timeField)), 0)"
                : "STATS "
                    + EARLIEST_TIME_COLUMN
                    + " = MIN(??timeField), "
                    + LATEST_TIME_COLUMN
                    + " = MAX(??timeField), "
                    + TOTAL_HITS_COLUMN
                    + " = COUNT(*)";
            return sourceWithPipeline(stats, timeFieldParams(sourceTimeField));
        }

        @Override
        public EsqlQueryWithParams sourceTimeFieldProbeQuery(String sourceTimeField) {
            // TS has to be followed by STATS, so it is probed with the over-time function the range summary uses.
            String pipeline = timeSeriesSource ? "STATS t = MAX(MAX_OVER_TIME(??timeField)) | LIMIT 0" : "KEEP ??timeField | LIMIT 0";
            return sourceWithPipeline(pipeline, timeFieldParams(sourceTimeField));
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
