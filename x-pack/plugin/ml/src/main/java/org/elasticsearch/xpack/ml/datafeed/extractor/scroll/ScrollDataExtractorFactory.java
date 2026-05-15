/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.scroll;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.TransportClearScrollAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class ScrollDataExtractorFactory implements DataExtractorFactory {

    private static final Logger logger = LogManager.getLogger(ScrollDataExtractorFactory.class);

    // This field type is not supported for scrolling datafeeds.
    private static final String AGGREGATE_METRIC_DOUBLE = "aggregate_metric_double";

    // Package-private record so tests can inspect queue contents
    record OrphanedScroll(String scrollId, long createdAtMillis, int retryAttempts) {}

    static final int MAX_ORPHAN_QUEUE_SIZE = 64;
    static final int MAX_ORPHAN_RETRIES = 5;
    static final long ORPHAN_TTL_MILLIS = TimeValue.timeValueMinutes(60).millis();

    private final Client client;
    private final DatafeedConfig datafeedConfig;
    private final QueryBuilder extraFilters;
    private final Job job;
    private final TimeBasedExtractedFields extractedFields;
    private final NamedXContentRegistry xContentRegistry;
    private final DatafeedTimingStatsReporter timingStatsReporter;

    /**
     * Scroll IDs that could not be cleared during a previous network disruption.
     * These survive across extractor lifetimes and are retried when the next
     * extractor successfully connects to the remote cluster.
     */
    final Deque<OrphanedScroll> orphanedScrolls = new ArrayDeque<>();

    ScrollDataExtractorFactory(
        Client client,
        DatafeedConfig datafeedConfig,
        QueryBuilder extraFilters,
        Job job,
        TimeBasedExtractedFields extractedFields,
        NamedXContentRegistry xContentRegistry,
        DatafeedTimingStatsReporter timingStatsReporter
    ) {
        this.client = Objects.requireNonNull(client);
        this.datafeedConfig = Objects.requireNonNull(datafeedConfig);
        this.extraFilters = extraFilters;
        this.job = Objects.requireNonNull(job);
        this.extractedFields = Objects.requireNonNull(extractedFields);
        this.xContentRegistry = xContentRegistry;
        this.timingStatsReporter = Objects.requireNonNull(timingStatsReporter);
    }

    /**
     * Records scroll IDs that a destroyed extractor could not clear during a network disruption (typically CCS).
     * <p>
     * Queuing is bounded: at most {@value #MAX_ORPHAN_QUEUE_SIZE} entries are retained (overflow evicts the
     * eldest with a WARN). {@link #retryClearOrphanedScrollIds()} is invoked when a new extractor starts
     * ({@link ScrollDataExtractor#initScroll(long)}) or during {@link ScrollDataExtractor#destroy()}; each
     * entry is dropped after {@link #ORPHAN_TTL_MILLIS} milliseconds since enqueue or after {@value #MAX_ORPHAN_RETRIES}
     * consecutive failed clears (evicted with a WARN).
     */
    void addOrphanedScrollIds(List<String> scrollIds) {
        long now = System.currentTimeMillis();
        for (String scrollId : scrollIds) {
            if (orphanedScrolls.size() >= MAX_ORPHAN_QUEUE_SIZE) {
                OrphanedScroll eldest = orphanedScrolls.poll();  // removes head (eldest)
                if (eldest != null) {
                    logger.warn(
                        "[{}] Orphan scroll queue overflow, dropping eldest entry aged {}ms",
                        job.getId(),
                        now - eldest.createdAtMillis()
                    );
                }
            }
            orphanedScrolls.add(new OrphanedScroll(scrollId, now, 0));
        }
    }

    /**
     * Returns {@code true} if there are orphaned scroll IDs waiting to be cleared.
     */
    boolean hasOrphanedScrollIds() {
        return orphanedScrolls.isEmpty() == false;
    }

    /**
     * Attempts to clear every queued orphaned scroll ID. Successfully cleared IDs are removed.
     * <p>
     * Failures increment per-entry retry bookkeeping and the ID stays in the queue for later passes until
     * {@link #ORPHAN_TTL_MILLIS} milliseconds elapse since it was queued or {@value #MAX_ORPHAN_RETRIES} failures are
     * reached, after which the ID is evicted with a WARN (no infinite retry).
     */
    void retryClearOrphanedScrollIds() {
        long now = System.currentTimeMillis();
        Iterator<OrphanedScroll> it = orphanedScrolls.iterator();
        while (it.hasNext()) {
            OrphanedScroll entry = it.next();
            // Evict if stale by age or max retries reached
            if (now - entry.createdAtMillis() > ORPHAN_TTL_MILLIS || entry.retryAttempts() >= MAX_ORPHAN_RETRIES) {
                logger.warn(
                    "[{}] Giving up on orphaned CCS scroll context [{}] after {} retries / {}ms — remote scroll may persist until TTL",
                    job.getId(),
                    entry.scrollId(),
                    entry.retryAttempts(),
                    now - entry.createdAtMillis()
                );
                it.remove();
                continue;
            }
            try {
                ClearScrollRequest request = new ClearScrollRequest();
                request.addScrollId(entry.scrollId());
                ClearScrollResponse response = ClientHelper.executeWithHeaders(
                    datafeedConfig.getHeaders(),
                    ClientHelper.ML_ORIGIN,
                    client,
                    () -> client.execute(TransportClearScrollAction.TYPE, request).actionGet()
                );
                if (response.isSucceeded() == false) {
                    throw new ElasticsearchException("Clear scroll returned failure for scroll [{}]", entry.scrollId());
                }
                it.remove();  // success: silently remove
            } catch (Exception e) {
                int newRetries = entry.retryAttempts() + 1;
                logger.debug("[{}] Retry {} for orphaned scroll [{}] still failed", job.getId(), newRetries, entry.scrollId());
                // Replace entry with incremented retry count
                it.remove();
                orphanedScrolls.add(new OrphanedScroll(entry.scrollId(), entry.createdAtMillis(), newRetries));
            }
        }
    }

    @Override
    public DataExtractor newExtractor(long start, long end) {
        QueryBuilder queryBuilder = datafeedConfig.getParsedQuery(xContentRegistry);
        if (extraFilters != null) {
            queryBuilder = QueryBuilders.boolQuery().filter(queryBuilder).filter(extraFilters);
        }
        ScrollDataExtractorContext dataExtractorContext = new ScrollDataExtractorContext(
            job.getId(),
            extractedFields,
            datafeedConfig.getIndices(),
            queryBuilder,
            datafeedConfig.getScriptFields(),
            datafeedConfig.getScrollSize(),
            start,
            end,
            datafeedConfig.getHeaders(),
            datafeedConfig.getIndicesOptions(),
            datafeedConfig.getRuntimeMappings()
        );
        return new ScrollDataExtractor(client, dataExtractorContext, timingStatsReporter, this);
    }

    public static void create(
        Client client,
        DatafeedConfig datafeed,
        QueryBuilder extraFilters,
        Job job,
        NamedXContentRegistry xContentRegistry,
        DatafeedTimingStatsReporter timingStatsReporter,
        ActionListener<DataExtractorFactory> listener
    ) {

        // Step 2. Construct the factory and notify listener
        ActionListener<FieldCapabilitiesResponse> fieldCapabilitiesHandler = ActionListener.wrap(fieldCapabilitiesResponse -> {
            if (fieldCapabilitiesResponse.getIndices().length == 0) {
                listener.onFailure(
                    ExceptionsHelper.badRequestException(
                        "datafeed [{}] cannot retrieve data because no index matches datafeed's indices {}",
                        datafeed.getId(),
                        datafeed.getIndices()
                    )
                );
                return;
            }
            Optional<String> optionalAggregatedMetricDouble = findFirstAggregatedMetricDoubleField(fieldCapabilitiesResponse);
            if (optionalAggregatedMetricDouble.isPresent()) {
                listener.onFailure(
                    ExceptionsHelper.badRequestException(
                        "field [{}] is of type [{}] and cannot be used in a datafeed without aggregations",
                        optionalAggregatedMetricDouble.get(),
                        AGGREGATE_METRIC_DOUBLE
                    )
                );
                return;
            }
            TimeBasedExtractedFields fields = TimeBasedExtractedFields.build(job, datafeed, fieldCapabilitiesResponse);
            listener.onResponse(
                new ScrollDataExtractorFactory(client, datafeed, extraFilters, job, fields, xContentRegistry, timingStatsReporter)
            );
        }, e -> {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof IndexNotFoundException notFound) {
                listener.onFailure(
                    new ResourceNotFoundException(
                        "datafeed [" + datafeed.getId() + "] cannot retrieve data because index " + notFound.getIndex() + " does not exist"
                    )
                );
            } else if (e instanceof IllegalArgumentException) {
                listener.onFailure(ExceptionsHelper.badRequestException("[" + datafeed.getId() + "] " + e.getMessage()));
            } else {
                listener.onFailure(e);
            }
        });

        // Step 1. Get field capabilities necessary to build the information of how to extract fields
        FieldCapabilitiesRequest fieldCapabilitiesRequest = new FieldCapabilitiesRequest();
        fieldCapabilitiesRequest.indices(datafeed.getIndices().toArray(new String[0])).indicesOptions(datafeed.getIndicesOptions());

        // Cannot get field caps on RT fields defined at search
        Set<String> runtimefields = datafeed.getRuntimeMappings().keySet();

        // We need capabilities for all fields matching the requested fields' parents so that we can work around
        // multi-fields that are not in source.
        String[] requestFields = job.allInputFields()
            .stream()
            .map(f -> MlStrings.getParentField(f) + "*")
            .filter(f -> runtimefields.contains(f) == false)
            .toArray(String[]::new);
        fieldCapabilitiesRequest.fields(requestFields);
        ClientHelper.<FieldCapabilitiesResponse>executeWithHeaders(datafeed.getHeaders(), ClientHelper.ML_ORIGIN, client, () -> {
            client.execute(TransportFieldCapabilitiesAction.TYPE, fieldCapabilitiesRequest, fieldCapabilitiesHandler);
            // This response gets discarded - the listener handles the real response
            return null;
        });
    }

    private static Optional<String> findFirstAggregatedMetricDoubleField(FieldCapabilitiesResponse fieldCapabilitiesResponse) {
        Map<String, Map<String, FieldCapabilities>> indexTofieldCapsMap = fieldCapabilitiesResponse.get();
        for (Map.Entry<String, Map<String, FieldCapabilities>> indexToFieldCaps : indexTofieldCapsMap.entrySet()) {
            for (Map.Entry<String, FieldCapabilities> typeToFieldCaps : indexToFieldCaps.getValue().entrySet()) {
                if (AGGREGATE_METRIC_DOUBLE.equals(typeToFieldCaps.getKey())) {
                    return Optional.of(typeToFieldCaps.getValue().getName());
                }
            }
        }
        return Optional.empty();
    }
}
