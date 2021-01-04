/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.v2;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeAction;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.HistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.analytics.mapper.HistogramFieldMapper;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.indexing.IterationResult;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupIndexerJobStats;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;
import org.elasticsearch.xpack.core.rollup.v2.RollupAction;
import org.elasticsearch.xpack.core.rollup.v2.RollupActionConfig;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * An abstract implementation of {@link AsyncTwoPhaseIndexer} that builds a rollup index incrementally.
 */
public class RollupV2Indexer extends AsyncTwoPhaseIndexer<Map<String, Object>, RollupIndexerJobStats> {
    private static final Logger logger = LogManager.getLogger(RollupV2Indexer.class);

    static final String AGGREGATION_NAME = RollupField.NAME;
    static final int PAGE_SIZE = 1000;

    private final RollupAction.Request request;
    private final Client client;
    private final Map<String, String> headers;
    private final CompositeAggregationBuilder compositeBuilder;
    private final String tmpIndex;
    private final ActionListener<Void> completionListener;

    /**
     * Ctr
     * @param client The Transport client
     * @param threadPool ThreadPool to use to fire the first request of a background job.
     * @param request The rollup request
     */
    RollupV2Indexer(Client client,
                    ThreadPool threadPool,
                    RollupAction.Request request,
                    Map<String, String> headers,
                    ActionListener<Void> completionListener) {
        super(threadPool, new AtomicReference<>(IndexerState.STOPPED), null, new RollupIndexerJobStats());
        this.client = client;
        this.request = request;
        this.headers = headers;
        this.compositeBuilder = createCompositeBuilder(this.request.getRollupConfig());
        this.tmpIndex = ".rolluptmp-" + this.request.getRollupIndex();
        this.completionListener = completionListener;
    }

    @Override
    protected String getJobId() {
        return "rollup_" + request.getRollupIndex();
    }

    @Override
    protected void onStart(long now, ActionListener<Boolean> listener) {
        try {
            createTempRollupIndex(ActionListener.wrap(resp -> listener.onResponse(true), e -> {
                completionListener.onFailure(new ElasticsearchException("Unable to start rollup. index creation failed", e));
                listener.onFailure(e);
            }));
        } catch (IOException e) {
            listener.onFailure(new ElasticsearchException("Unable to start rollup. index creation failed", e));
        }
    }

    XContentBuilder getMapping() throws IOException {
        GroupConfig groupConfig = request.getRollupConfig().getGroupConfig();
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("properties");
        String dateField = groupConfig.getDateHistogram().getField();
        HistogramGroupConfig histogramGroupConfig = groupConfig.getHistogram();
        TermsGroupConfig termsGroupConfig = groupConfig.getTerms();
        List<MetricConfig> metricConfigs = request.getRollupConfig().getMetricsConfig();

        builder.startObject(dateField).field("type", DateFieldMapper.CONTENT_TYPE).endObject();

        if (histogramGroupConfig != null) {
            for (String field : histogramGroupConfig.getFields()) {
                builder.startObject(field).field("type", HistogramFieldMapper.CONTENT_TYPE).endObject();
            }
        }

        if (termsGroupConfig != null) {
            for (String field : termsGroupConfig.getFields()) {
                // TODO(talevy): not just keyword, can be a number?
                builder.startObject(field).field("type", KeywordFieldMapper.CONTENT_TYPE).endObject();
            }
        }

        for (MetricConfig metricConfig : metricConfigs) {
            List<String> metrics = normalizeMetrics(metricConfig.getMetrics());
            String defaultMetric = metrics.contains("value_count") ? "value_count" : metrics.get(0);
            builder.startObject(metricConfig.getField())
                .field("type", "aggregate_metric_double")
                .array("metrics", metrics.toArray())
                .field("default_metric", defaultMetric)
                .endObject();
        }


        return builder.endObject().endObject();
    }

    void createTempRollupIndex(ActionListener<CreateIndexResponse> listener) throws IOException {
        CreateIndexRequest req = new CreateIndexRequest(tmpIndex, Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_HIDDEN, true).build())
            .mapping(getMapping());
        ClientHelper.executeWithHeadersAsync(headers, ClientHelper.ROLLUP_ORIGIN, client, CreateIndexAction.INSTANCE, req, listener);
    }

    @Override
    protected void doNextSearch(long waitTimeInNanos, ActionListener<SearchResponse> nextPhase) {
        ClientHelper.executeWithHeadersAsync(headers, ClientHelper.ROLLUP_ORIGIN, client, SearchAction.INSTANCE,
            buildSearchRequest(waitTimeInNanos), nextPhase);
    }

    @Override
    protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
        ClientHelper.executeWithHeadersAsync(headers, ClientHelper.ROLLUP_ORIGIN, client, BulkAction.INSTANCE, request, nextPhase);
    }

    @Override
    protected void doSaveState(IndexerState state, Map<String, Object> stringObjectMap, Runnable next) {
        assert state == IndexerState.INDEXING || state == IndexerState.STARTED || state == IndexerState.STOPPED;
        next.run();
    }

    @Override
    protected void onFailure(Exception exc) {
        completionListener.onFailure(exc);
    }

    @Override
    protected void onFinish(ActionListener<Void> listener) {
        // "shrink index"
        ResizeRequest resizeRequest = new ResizeRequest(request.getRollupIndex(), tmpIndex);
        resizeRequest.setResizeType(ResizeType.CLONE);
        resizeRequest.getTargetIndexRequest()
            .settings(Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, false).build());
        UpdateSettingsRequest updateSettingsReq = new UpdateSettingsRequest(
            Settings.builder().put("index.blocks.write", true).build(), tmpIndex);
        ClientHelper.executeWithHeadersAsync(headers, ClientHelper.ROLLUP_ORIGIN, client,
            UpdateSettingsAction.INSTANCE, updateSettingsReq,
            ActionListener.wrap(r -> {
                ClientHelper.executeWithHeadersAsync(headers, ClientHelper.ROLLUP_ORIGIN, client,
                    ResizeAction.INSTANCE, resizeRequest,
                    ActionListener.wrap(rr -> { deleteTmpIndex(listener); }, e -> { deleteTmpIndex(listener); }));
                }, e -> { deleteTmpIndex(listener); }));
    }

    private void deleteTmpIndex(ActionListener<Void> listener) {
        ClientHelper.executeWithHeadersAsync(headers, ClientHelper.ROLLUP_ORIGIN, client,
            DeleteIndexAction.INSTANCE, new DeleteIndexRequest(tmpIndex),
            ActionListener.wrap(r -> {
                listener.onResponse(null);
                completionListener.onResponse(null);
            }, e -> {
                listener.onFailure(e);
                completionListener.onFailure(e);
            }));
    }

    @Override
    protected void onStop() {
        completionListener.onFailure(new ElasticsearchException("rollup stopped before completion"));
    }

    @Override
    protected void onAbort() {
        completionListener.onFailure(new ElasticsearchException("rollup stopped before completion"));
    }

    protected SearchRequest buildSearchRequest(long waitTimeInNanos) {
        final Map<String, Object> position = getPosition();
        SearchSourceBuilder searchSource = new SearchSourceBuilder()
                .size(0)
                .trackTotalHits(false)
                .aggregation(compositeBuilder.aggregateAfter(position));
        return new SearchRequest(request.getSourceIndex()).source(searchSource);
    }

    @Override
    protected IterationResult<Map<String, Object>> doProcess(SearchResponse searchResponse) {
        final CompositeAggregation response = searchResponse.getAggregations().get(AGGREGATION_NAME);

        if (response.getBuckets().isEmpty()) {
            // do not reset the position as we want to continue from where we stopped
            return new IterationResult<>(Collections.emptyList(), getPosition(), true);
        }

        return new IterationResult<>(processBuckets(response, tmpIndex, getStats()),
                response.afterKey(), response.getBuckets().isEmpty());
    }

    static List<IndexRequest> processBuckets(CompositeAggregation agg, String rollupIndex, RollupIndexerJobStats stats) {

        return agg.getBuckets().stream().map(b ->{
            stats.incrementNumDocuments(b.getDocCount());

            // Put the composite keys into a treemap so that the key iteration order is consistent
            // TODO would be nice to avoid allocating this treemap in the future
            TreeMap<String, Object> keys = new TreeMap<>(b.getKey());
            List<Aggregation> metrics = b.getAggregations().asList();

            Map<String, Object> doc = new HashMap<>(1 + keys.size() + metrics.size());
            doc.put("_doc_count", b.getDocCount());
            keys.forEach(doc::put);
            Map<String, Map<String, Double>> metricFields = new HashMap<>();
            metrics.forEach(a -> {
                String name = a.getName();
                int split = name.lastIndexOf("-");
                assert split > 0;
                String fieldName = name.substring(0, split);
                String metricType = name.substring(split + 1);
                if (a instanceof InternalNumericMetricsAggregation.SingleValue) {
                    double value = ((InternalNumericMetricsAggregation.SingleValue) a).value();
                    if (metricFields.containsKey(fieldName)) {
                        metricFields.get(fieldName).put(metricType, value);
                    } else {
                        Map<String, Double> vals = new HashMap<>();
                        vals.put(metricType, value);
                        metricFields.put(fieldName, vals);
                    }
                } else {
                    throw new IllegalStateException("invalid metric type [" + a.getClass().getName() + "]");
                }
            });
            doc.putAll(metricFields);
            IndexRequest request = new IndexRequest(rollupIndex);
            request.source(doc);
            return request;
        }).collect(Collectors.toList());
    }

    /**
     * Creates a skeleton {@link CompositeAggregationBuilder} from the provided job config.
     * @param config The config for the job.
     * @return The composite aggregation that creates the rollup buckets
     */
    private CompositeAggregationBuilder createCompositeBuilder(RollupActionConfig config) {
        final GroupConfig groupConfig = config.getGroupConfig();
        List<CompositeValuesSourceBuilder<?>> builders = createValueSourceBuilders(groupConfig);

        CompositeAggregationBuilder composite = new CompositeAggregationBuilder(AGGREGATION_NAME, builders);

        List<AggregationBuilder> aggregations = createAggregationBuilders(config.getMetricsConfig());
        aggregations.forEach(composite::subAggregation);

        final Map<String, Object> metadata = createMetadata(groupConfig);
        if (metadata.isEmpty() == false) {
            composite.setMetadata(metadata);
        }
        composite.size(PAGE_SIZE);

        return composite;
    }

    static Map<String, Object> createMetadata(final GroupConfig groupConfig) {
        final Map<String, Object> metadata = new HashMap<>();
        if (groupConfig != null) {
            // Add all the metadata in order: date_histo -> histo
            final DateHistogramGroupConfig dateHistogram = groupConfig.getDateHistogram();
            metadata.put(RollupField.formatMetaField(RollupField.INTERVAL), dateHistogram.getInterval().toString());

            final HistogramGroupConfig histogram = groupConfig.getHistogram();
            if (histogram != null) {
                metadata.put(RollupField.formatMetaField(RollupField.INTERVAL), histogram.getInterval());
            }
        }
        return metadata;
    }

    public static List<CompositeValuesSourceBuilder<?>> createValueSourceBuilders(final GroupConfig groupConfig) {
        final List<CompositeValuesSourceBuilder<?>> builders = new ArrayList<>();
        // Add all the agg builders to our request in order: date_histo -> histo -> terms
        if (groupConfig != null) {
            final DateHistogramGroupConfig dateHistogram = groupConfig.getDateHistogram();
            builders.addAll(createValueSourceBuilders(dateHistogram));

            final HistogramGroupConfig histogram = groupConfig.getHistogram();
            builders.addAll(createValueSourceBuilders(histogram));

            final TermsGroupConfig terms = groupConfig.getTerms();
            builders.addAll(createValueSourceBuilders(terms));
        }
        return Collections.unmodifiableList(builders);
    }

    public static List<CompositeValuesSourceBuilder<?>> createValueSourceBuilders(final DateHistogramGroupConfig dateHistogram) {
        final String dateHistogramField = dateHistogram.getField();
        final DateHistogramValuesSourceBuilder dateHistogramBuilder = new DateHistogramValuesSourceBuilder(dateHistogramField);
        if (dateHistogram instanceof DateHistogramGroupConfig.FixedInterval) {
            dateHistogramBuilder.fixedInterval(dateHistogram.getInterval());
        } else if (dateHistogram instanceof DateHistogramGroupConfig.CalendarInterval) {
            dateHistogramBuilder.calendarInterval(dateHistogram.getInterval());
        } else {
            dateHistogramBuilder.dateHistogramInterval(dateHistogram.getInterval());
        }
        dateHistogramBuilder.field(dateHistogramField);
        dateHistogramBuilder.timeZone(ZoneId.of(dateHistogram.getTimeZone()));
        return Collections.singletonList(dateHistogramBuilder);
    }

    public static List<CompositeValuesSourceBuilder<?>> createValueSourceBuilders(final HistogramGroupConfig histogram) {
        final List<CompositeValuesSourceBuilder<?>> builders = new ArrayList<>();
        if (histogram != null) {
            for (String field : histogram.getFields()) {
                final HistogramValuesSourceBuilder histogramBuilder = new HistogramValuesSourceBuilder(field);
                histogramBuilder.interval(histogram.getInterval());
                histogramBuilder.field(field);
                histogramBuilder.missingBucket(true);
                builders.add(histogramBuilder);
            }
        }
        return Collections.unmodifiableList(builders);
    }

    public static List<CompositeValuesSourceBuilder<?>> createValueSourceBuilders(final TermsGroupConfig terms) {
        final List<CompositeValuesSourceBuilder<?>> builders = new ArrayList<>();
        if (terms != null) {
            for (String field : terms.getFields()) {
                final TermsValuesSourceBuilder termsBuilder = new TermsValuesSourceBuilder(field);
                termsBuilder.field(field);
                termsBuilder.missingBucket(true);
                builders.add(termsBuilder);
            }
        }
        return Collections.unmodifiableList(builders);
    }

    /**
     * This returns a set of aggregation builders which represent the configured
     * set of metrics. Used to iterate over historical data.
     */
    static List<AggregationBuilder> createAggregationBuilders(final List<MetricConfig> metricsConfigs) {
        final List<AggregationBuilder> builders = new ArrayList<>();
        if (metricsConfigs != null) {
            for (MetricConfig metricConfig : metricsConfigs) {
                final List<String> metrics = metricConfig.getMetrics();
                final List<String> normalizedMetrics = normalizeMetrics(metrics);
                if (normalizedMetrics.isEmpty() == false) {
                    final String field = metricConfig.getField();
                    for (String metric : normalizedMetrics) {
                        ValuesSourceAggregationBuilder.LeafOnly<? extends ValuesSource, ? extends AggregationBuilder> newBuilder;
                        if (metric.equals(MetricConfig.MIN.getPreferredName())) {
                            newBuilder = new MinAggregationBuilder(field + "-min");
                        } else if (metric.equals(MetricConfig.MAX.getPreferredName())) {
                            newBuilder = new MaxAggregationBuilder(field + "-max");
                        } else if (metric.equals(MetricConfig.SUM.getPreferredName())) {
                            newBuilder = new SumAggregationBuilder(field + "-sum");
                        } else if (metric.equals(MetricConfig.VALUE_COUNT.getPreferredName())) {
                            newBuilder = new ValueCountAggregationBuilder(field + "-value_count");
                        } else {
                            throw new IllegalArgumentException("Unsupported metric type [" + metric + "]");
                        }
                        newBuilder.field(field);
                        builders.add(newBuilder);
                    }
                }
            }
        }
        return Collections.unmodifiableList(builders);
    }

    static List<String> normalizeMetrics(List<String> metrics) {
        List<String> newMetrics = new ArrayList<>(metrics);
        // avg = sum + value_count
        if (newMetrics.remove(MetricConfig.AVG.getPreferredName())) {
            if (newMetrics.contains(MetricConfig.VALUE_COUNT.getPreferredName()) == false) {
                newMetrics.add(MetricConfig.VALUE_COUNT.getPreferredName());
            }
            if (newMetrics.contains(MetricConfig.SUM.getPreferredName()) == false) {
                newMetrics.add(MetricConfig.SUM.getPreferredName());
            }
        }
        return newMetrics;
    }
}

