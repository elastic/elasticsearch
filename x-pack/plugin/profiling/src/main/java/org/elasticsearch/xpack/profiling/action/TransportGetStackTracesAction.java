/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.profiling.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.RefCountAwareThreadedActionListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.countedterms.CountedTermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.random.RandomSamplerAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xpack.profiling.ProfilingPlugin;
import org.elasticsearch.xpack.profiling.persistence.EventsIndex;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class TransportGetStackTracesAction extends TransportAction<GetStackTracesRequest, GetStackTracesResponse> {
    private static final Logger log = LogManager.getLogger(TransportGetStackTracesAction.class);

    public static final Setting<Integer> PROFILING_MAX_STACKTRACE_QUERY_SLICES = Setting.intSetting(
        "xpack.profiling.query.stacktrace.max_slices",
        16,
        1,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> PROFILING_MAX_DETAIL_QUERY_SLICES = Setting.intSetting(
        "xpack.profiling.query.details.max_slices",
        16,
        1,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> PROFILING_QUERY_REALTIME = Setting.boolSetting(
        "xpack.profiling.query.realtime",
        true,
        Setting.Property.NodeScope
    );

    /**
     * K/V indices (such as profiling-stacktraces) are assumed to contain data from their creation date until the creation date
     * of the next index that is created by rollover. Due to client-side caching of K/V data we need to extend the validity period
     * of the prior index by this time. This means that for queries that cover a time period around the time when a new index has
     * been created we will query not only the new index but also the prior one. The client-side parameters that influence cache duration
     * are <code>elfInfoCacheTTL</code> for executables (default: 6 hours) and <code>traceExpirationTimeout</code> for stack
     * traces (default: 3 hours).
     */
    public static final Setting<TimeValue> PROFILING_KV_INDEX_OVERLAP = Setting.positiveTimeSetting(
        "xpack.profiling.kv_index.overlap",
        TimeValue.timeValueHours(6),
        Setting.Property.NodeScope
    );

    /**
     * The maximum number of items in the response that we support.
     * The down-sampling of the events indices limits the number of items to ~100k.
     * We use a higher number to be on the safe side.
     */
    private static final int MAX_TRACE_EVENTS_RESULT_SIZE = 150_000;

    /**
     * Users may provide a custom field via the API that is used to sub-divide profiling events. This is useful in the context of TopN
     * where we want to provide additional breakdown of where a certain function has been called (e.g. a certain service or transaction).
     */
    private static final String CUSTOM_EVENT_SUB_AGGREGATION_NAME = "custom_event_group";

    /**
     * This is the default sampling rate for profiling events that we use if no sampling rate is
     * stored in the backend (backwards compatibility).
     */
    public static final double DEFAULT_SAMPLING_FREQUENCY = 19.0d;

    private final NodeClient nodeClient;
    private final ProfilingLicenseChecker licenseChecker;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final Executor responseExecutor;

    private final KvIndexResolver resolver;
    private final int desiredSlices;
    private final int desiredDetailSlices;
    private final boolean realtime;

    @Inject
    public TransportGetStackTracesAction(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        NodeClient nodeClient,
        ProfilingLicenseChecker licenseChecker,
        IndexNameExpressionResolver resolver
    ) {
        super(GetStackTracesAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.nodeClient = nodeClient;
        this.licenseChecker = licenseChecker;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.responseExecutor = threadPool.executor(ProfilingPlugin.PROFILING_THREAD_POOL_NAME);
        this.resolver = new KvIndexResolver(resolver, PROFILING_KV_INDEX_OVERLAP.get(settings));
        this.desiredSlices = PROFILING_MAX_STACKTRACE_QUERY_SLICES.get(settings);
        this.desiredDetailSlices = PROFILING_MAX_DETAIL_QUERY_SLICES.get(settings);
        this.realtime = PROFILING_QUERY_REALTIME.get(settings);
    }

    @Override
    protected void doExecute(Task task, GetStackTracesRequest request, ActionListener<GetStackTracesResponse> submitListener) {
        licenseChecker.requireSupportedLicense();
        assert task instanceof CancellableTask;
        final CancellableTask submitTask = (CancellableTask) task;
        GetStackTracesResponseBuilder responseBuilder = new GetStackTracesResponseBuilder(request);
        Client client = new ParentTaskAssigningClient(this.nodeClient, transportService.getLocalNode(), submitTask);
        if (request.isUserProvidedIndices()) {
            searchGenericEvents(submitTask, client, request, submitListener, responseBuilder);
        } else {
            searchProfilingEvents(submitTask, client, request, submitListener, responseBuilder);
        }
    }

    private void searchProfilingEvents(
        CancellableTask submitTask,
        Client client,
        GetStackTracesRequest request,
        ActionListener<GetStackTracesResponse> submitListener,
        GetStackTracesResponseBuilder responseBuilder
    ) {
        StopWatch watch = new StopWatch("getResampledIndex");
        EventsIndex mediumDownsampled = EventsIndex.MEDIUM_DOWNSAMPLED;
        client.prepareSearch(mediumDownsampled.getName())
            .setSize(0)
            .setQuery(request.getQuery())
            .setTrackTotalHits(true)
            .execute(ActionListener.wrap(searchResponse -> {
                long sampleCount = searchResponse.getHits().getTotalHits().value();
                EventsIndex resampledIndex = mediumDownsampled.getResampledIndex(request.getSampleSize(), sampleCount);
                log.debug(
                    "User requested [{}] samples, [{}] samples matched in [{}]. Picking [{}]",
                    request.getSampleSize(),
                    sampleCount,
                    mediumDownsampled,
                    resampledIndex
                );
                log.debug(watch::report);
                searchEventGroupedByStackTrace(submitTask, client, request, submitListener, responseBuilder, resampledIndex);
            }, e -> {
                // All profiling-events data streams are created lazily. In a relatively empty cluster it can happen that there are so few
                // data that we need to resort to the "full" events stream. As this is an edge case we'd rather fail instead of prematurely
                // checking for existence in all cases.
                if (e instanceof IndexNotFoundException) {
                    String missingIndex = ((IndexNotFoundException) e).getIndex().getName();
                    EventsIndex fullIndex = EventsIndex.FULL_INDEX;
                    log.debug("Index [{}] does not exist. Using [{}] instead.", missingIndex, fullIndex.getName());
                    searchEventGroupedByStackTrace(submitTask, client, request, submitListener, responseBuilder, fullIndex);
                } else {
                    submitListener.onFailure(e);
                }
            }));
    }

    private void searchGenericEvents(
        CancellableTask submitTask,
        Client client,
        GetStackTracesRequest request,
        ActionListener<GetStackTracesResponse> submitListener,
        GetStackTracesResponseBuilder responseBuilder
    ) {
        StopWatch watch = new StopWatch("getSamplingRate");
        client.prepareSearch(request.getIndices())
            .setSize(0)
            .setTrackTotalHits(true)
            .setRequestCache(true)
            .setPreference(String.valueOf(request.hashCode()))
            .setQuery(request.getQuery())
            .execute(ActionListener.wrap(searchResponse -> {
                long sampleCount = searchResponse.getHits().getTotalHits().value();
                int requestedSampleCount = request.getSampleSize();
                // random sampler aggregation does not support sampling rates between 0.5 and 1.0 -> clamp to 1.0
                if (sampleCount <= requestedSampleCount * 2L) {
                    responseBuilder.setSamplingRate(1.0d);
                } else {
                    responseBuilder.setSamplingRate((double) requestedSampleCount / (double) sampleCount);
                }
                log.debug(watch::report);
                log.debug(
                    "User requested [{}] samples, [{}] samples matched in [{}]. Sampling rate is [{}].",
                    requestedSampleCount,
                    sampleCount,
                    request.getIndices(),
                    responseBuilder.getSamplingRate()
                );
                if (sampleCount > 0) {
                    searchGenericEventGroupedByStackTrace(submitTask, client, request, submitListener, responseBuilder);
                } else {
                    submitListener.onResponse(responseBuilder.build());
                }
            }, submitListener::onFailure));
    }

    private void searchGenericEventGroupedByStackTrace(
        CancellableTask submitTask,
        Client client,
        GetStackTracesRequest request,
        ActionListener<GetStackTracesResponse> submitListener,
        GetStackTracesResponseBuilder responseBuilder
    ) {
        CountedTermsAggregationBuilder groupByStackTraceId = new CountedTermsAggregationBuilder("group_by").size(
            MAX_TRACE_EVENTS_RESULT_SIZE
        ).field(request.getStackTraceIdsField());
        SubGroupCollector subGroups = SubGroupCollector.attach(groupByStackTraceId, request.getAggregationFields());
        RandomSamplerAggregationBuilder randomSampler = new RandomSamplerAggregationBuilder("sample").setSeed(request.hashCode())
            .setProbability(responseBuilder.getSamplingRate())
            .subAggregation(groupByStackTraceId);
        // shard seed is only set in tests and ensures consistent results
        if (request.getShardSeed() != null) {
            randomSampler.setShardSeed(request.getShardSeed());
        }
        client.prepareSearch(request.getIndices())
            .setTrackTotalHits(false)
            .setSize(0)
            // take advantage of request cache and keep a consistent order for the same request
            .setRequestCache(true)
            .setPreference(String.valueOf(request.hashCode()))
            .setQuery(request.getQuery())
            .addAggregation(new MinAggregationBuilder("min_time").field("@timestamp"))
            .addAggregation(new MaxAggregationBuilder("max_time").field("@timestamp"))
            .addAggregation(randomSampler)
            .execute(handleEventsGroupedByStackTrace(submitTask, client, responseBuilder, submitListener, searchResponse -> {
                long totalSamples = 0;
                SingleBucketAggregation sample = searchResponse.getAggregations().get("sample");
                Terms stacktraces = sample.getAggregations().get("group_by");

                // When we retrieve data for generic events, we need to adapt the handler similar to searchEventGroupedByStackTrace().

                // aggregation
                Map<TraceEventID, TraceEvent> stackTraceEvents = new HashMap<>();
                for (Terms.Bucket stacktraceBucket : stacktraces.getBuckets()) {
                    long count = stacktraceBucket.getDocCount();
                    totalSamples += count;

                    String stackTraceID = stacktraceBucket.getKeyAsString();

                    TraceEventID eventID = new TraceEventID("", "", "", stackTraceID, DEFAULT_SAMPLING_FREQUENCY);
                    TraceEvent event = stackTraceEvents.computeIfAbsent(eventID, k -> new TraceEvent());
                    event.count += count;
                    subGroups.collectResults(stacktraceBucket, event);
                }
                responseBuilder.setTotalSamples(totalSamples);
                log.debug("Found [{}] stacktrace events.", stackTraceEvents.size());
                return stackTraceEvents;
            }));
    }

    private void searchEventGroupedByStackTrace(
        CancellableTask submitTask,
        Client client,
        GetStackTracesRequest request,
        ActionListener<GetStackTracesResponse> submitListener,
        GetStackTracesResponseBuilder responseBuilder,
        EventsIndex eventsIndex
    ) {
        // We have nested aggregations, which in theory might blow up to MAX_TRACE_EVENTS_RESULT_SIZE^2 items
        // reported. But we know that the total number of items is limited by our down-sampling to
        // a maximum of ~100k (MAX_TRACE_EVENTS_RESULT_SIZE is higher to be on the safe side).
        responseBuilder.setSamplingRate(eventsIndex.getSampleRate());
        TermsAggregationBuilder groupByStackTraceId = new TermsAggregationBuilder("group_by")
            // 'size' should be max 100k, but might be slightly more. Better be on the safe side.
            .size(MAX_TRACE_EVENTS_RESULT_SIZE)
            .field("Stacktrace.id")
            // 'execution_hint: map' skips the slow building of ordinals that we don't need.
            // Especially with high cardinality fields, this makes aggregations really slow.
            .executionHint("map")
            .subAggregation(new SumAggregationBuilder("count").field("Stacktrace.count"));
        TermsAggregationBuilder groupByHostId = new TermsAggregationBuilder("group_by")
            // 'size' specifies the max number of host ID we support per request.
            .size(MAX_TRACE_EVENTS_RESULT_SIZE)
            .field("host.id")
            // missing("") is used to include documents where the field is missing.
            .missing("")
            // 'execution_hint: map' skips the slow building of ordinals that we don't need.
            // Especially with high cardinality fields, this makes aggregations really slow.
            .executionHint("map")
            .subAggregation(groupByStackTraceId);
        TermsAggregationBuilder groupByThreadName = new TermsAggregationBuilder("group_by")
            // 'size' specifies the max number of host ID we support per request.
            .size(MAX_TRACE_EVENTS_RESULT_SIZE)
            .field("process.thread.name")
            // missing("") is used to include documents where the field is missing.
            .missing("")
            // 'execution_hint: map' skips the slow building of ordinals that we don't need.
            // Especially with high cardinality fields, this makes aggregations really slow.
            .executionHint("map")
            .subAggregation(groupByHostId);
        TermsAggregationBuilder groupByExecutableName = new TermsAggregationBuilder("group_by")
            // 'size' specifies the max number of host IDs we support per request.
            .size(MAX_TRACE_EVENTS_RESULT_SIZE)
            .field("process.executable.name")
            // missing("") is used to include documents where the field is missing.
            .missing("")
            // 'execution_hint: map' skips the slow building of ordinals that we don't need.
            // Especially with high cardinality fields, this makes aggregations really slow.
            .executionHint("map")
            .subAggregation(groupByThreadName);
        SubGroupCollector subGroups = SubGroupCollector.attach(groupByStackTraceId, request.getAggregationFields());
        client.prepareSearch(eventsIndex.getName())
            .setTrackTotalHits(false)
            .setSize(0)
            // take advantage of request cache and keep a consistent order for the same request
            .setRequestCache(true)
            .setPreference(String.valueOf(request.hashCode()))
            .setQuery(request.getQuery())
            .addAggregation(new MinAggregationBuilder("min_time").field("@timestamp"))
            .addAggregation(new MaxAggregationBuilder("max_time").field("@timestamp"))
            .addAggregation(
                new TermsAggregationBuilder("group_by")
                    // 'size' specifies the max number of host ID we support per request.
                    .size(MAX_TRACE_EVENTS_RESULT_SIZE)
                    .field("Stacktrace.sampling_frequency")
                    // missing(DEFAULT_SAMPLING_RATE) is used to include documents where the field is missing.
                    .missing((long) DEFAULT_SAMPLING_FREQUENCY)
                    // 'execution_hint: map' skips the slow building of ordinals that we don't need.
                    // Especially with high cardinality fields, this makes aggregations really slow.
                    .executionHint("map")
                    .subAggregation(groupByExecutableName)
                    .subAggregation(new SumAggregationBuilder("total_count").field("Stacktrace.count"))
            )
            .addAggregation(new SumAggregationBuilder("total_count").field("Stacktrace.count"))
            .execute(handleEventsGroupedByStackTrace(submitTask, client, responseBuilder, submitListener, searchResponse -> {
                // The count values for events are scaled up to the highest sampling frequency.
                // For example, if the highest sampling frequency is 100, an event with frequency=20 and count=1
                // will be upscaled to count=5 (100/20 * count).
                // For this, we need to find the highest frequency in the result set.
                long maxSamplingFrequency = 0;
                Terms samplingFrequencies = searchResponse.getAggregations().get("group_by");
                for (Terms.Bucket samplingFrequencyBucket : samplingFrequencies.getBuckets()) {
                    final double samplingFrequency = samplingFrequencyBucket.getKeyAsNumber().doubleValue();
                    if (samplingFrequency > maxSamplingFrequency) {
                        maxSamplingFrequency = (long) samplingFrequency;
                    }
                }

                // Calculate a scaled-up total count (scaled up to the highest sampling frequency).
                long totalCount = 0;
                for (Terms.Bucket samplingFrequencyBucket : samplingFrequencies.getBuckets()) {
                    InternalNumericMetricsAggregation.SingleValue count = samplingFrequencyBucket.getAggregations().get("total_count");
                    final double samplingFrequency = samplingFrequencyBucket.getKeyAsNumber().doubleValue();
                    final double samplingFactor = maxSamplingFrequency / samplingFrequency;
                    totalCount += Math.round(count.value() * samplingFactor);
                }

                Resampler resampler = new Resampler(request, responseBuilder.getSamplingRate(), totalCount);

                // Sort items lexicographically to access Lucene's term dictionary more efficiently when issuing an mget request.
                // The term dictionary is lexicographically sorted, and using the same order reduces the number of page faults
                // needed to load it.
                long totalFinalCount = 0;
                Map<TraceEventID, TraceEvent> stackTraceEvents = new HashMap<>(MAX_TRACE_EVENTS_RESULT_SIZE);

                // Walk over all nested aggregations.
                // The outermost aggregation is the sampling frequency.
                // The next level is the executable name, followed by the thread name, host ID and stacktrace ID.
                // the innermost aggregation contains the count of samples for each stacktrace ID.
                for (Terms.Bucket samplingFrequencyBucket : samplingFrequencies.getBuckets()) {
                    final double samplingFrequency = samplingFrequencyBucket.getKeyAsNumber().doubleValue();
                    final double samplingFactor = maxSamplingFrequency / samplingFrequency;

                    Terms executableNames = samplingFrequencyBucket.getAggregations().get("group_by");
                    for (Terms.Bucket executableBucket : executableNames.getBuckets()) {
                        String executableName = executableBucket.getKeyAsString();

                        Terms threads = executableBucket.getAggregations().get("group_by");
                        for (Terms.Bucket threadBucket : threads.getBuckets()) {
                            String threadName = threadBucket.getKeyAsString();

                            Terms hosts = threadBucket.getAggregations().get("group_by");
                            for (Terms.Bucket hostBucket : hosts.getBuckets()) {
                                String hostID = hostBucket.getKeyAsString();

                                Terms stacktraces = hostBucket.getAggregations().get("group_by");
                                for (Terms.Bucket stacktraceBucket : stacktraces.getBuckets()) {
                                    Sum count = stacktraceBucket.getAggregations().get("count");
                                    int finalCount = resampler.adjustSampleCount((int) Math.round(count.value() * samplingFactor));
                                    if (finalCount <= 0) {
                                        continue;
                                    }

                                    totalFinalCount += finalCount;

                                    String stackTraceID = stacktraceBucket.getKeyAsString();
                                    TraceEventID eventID = new TraceEventID(
                                        executableName,
                                        threadName,
                                        hostID,
                                        stackTraceID,
                                        maxSamplingFrequency
                                    );
                                    TraceEvent event = stackTraceEvents.computeIfAbsent(eventID, k -> new TraceEvent());
                                    event.count += finalCount;
                                    subGroups.collectResults(stacktraceBucket, event);
                                }
                            }
                        }
                    }
                }
                responseBuilder.setTotalSamples(totalFinalCount);
                log.debug(
                    "Found [{}] stacktrace events, resampled with sample rate [{}] to [{}] events ([{}] unique stack traces).",
                    totalCount,
                    responseBuilder.getSamplingRate(),
                    totalFinalCount,
                    stackTraceEvents.size()
                );
                return stackTraceEvents;
            }));
    }

    private ActionListener<SearchResponse> handleEventsGroupedByStackTrace(
        CancellableTask submitTask,
        Client client,
        GetStackTracesResponseBuilder responseBuilder,
        ActionListener<GetStackTracesResponse> submitListener,
        Function<SearchResponse, Map<TraceEventID, TraceEvent>> stacktraceCollector
    ) {
        StopWatch watch = new StopWatch("eventsGroupedByStackTrace");
        return ActionListener.wrap(searchResponse -> {
            long minTime = getAggValueAsLong(searchResponse, "min_time");
            long maxTime = getAggValueAsLong(searchResponse, "max_time");

            Map<TraceEventID, TraceEvent> stackTraceEvents = stacktraceCollector.apply(searchResponse);

            log.debug(watch::report);
            if (stackTraceEvents.isEmpty() == false) {
                responseBuilder.setStart(Instant.ofEpochMilli(minTime));
                responseBuilder.setEnd(Instant.ofEpochMilli(maxTime));
                responseBuilder.setStackTraceEvents(stackTraceEvents);
                retrieveStackTraces(submitTask, client, responseBuilder, submitListener);
            } else {
                submitListener.onResponse(responseBuilder.build());
            }
        }, e -> {
            // Data streams are created lazily; if even the "full" index does not exist no data have been indexed yet.
            if (e instanceof IndexNotFoundException) {
                log.debug("Index [{}] does not exist. Returning empty response.", ((IndexNotFoundException) e).getIndex());
                submitListener.onResponse(responseBuilder.build());
            } else {
                submitListener.onFailure(e);
            }
        });
    }

    private static long getAggValueAsLong(SearchResponse searchResponse, String field) {
        InternalNumericMetricsAggregation.SingleValue x = searchResponse.getAggregations().get(field);
        return Math.round(x.value());
    }

    private void retrieveStackTraces(
        CancellableTask submitTask,
        Client client,
        GetStackTracesResponseBuilder responseBuilder,
        ActionListener<GetStackTracesResponse> submitListener
    ) {
        if (submitTask.notifyIfCancelled(submitListener)) {
            return;
        }
        Set<String> stacktraceIds = new TreeSet<>();
        Set<String> hostIds = new TreeSet<>();
        for (TraceEventID id : responseBuilder.getStackTraceEvents().keySet()) {
            stacktraceIds.add(id.stacktraceID());
            hostIds.add(id.hostID());
        }
        log.info("Using [{}] hostIds and [{}] stacktraceIds.", hostIds.size(), stacktraceIds.size());

        ClusterState clusterState = clusterService.state();
        List<Index> indices = resolver.resolve(clusterState, "profiling-stacktraces", responseBuilder.getStart(), responseBuilder.getEnd());
        // Avoid parallelism if there is potential we are on spinning disks (frozen tier uses searchable snapshots)
        int sliceCount = IndexAllocation.isAnyOnWarmOrColdTier(clusterState, indices) ? 1 : desiredSlices;
        log.trace("Using [{}] slice(s) to lookup stacktraces.", sliceCount);
        List<List<String>> slicedEventIds = sliced(new ArrayList<>(stacktraceIds), sliceCount);

        StackTraceHandler handler = new StackTraceHandler(
            submitTask,
            clusterState,
            client,
            responseBuilder,
            submitListener,
            stacktraceIds.size(),
            // We need to expect a set of slices for each resolved index, plus one for the host metadata.
            slicedEventIds.size() * indices.size() + (hostIds.isEmpty() ? 0 : 1),
            hostIds.size()
        );
        for (List<String> slice : slicedEventIds) {
            mget(client, indices, slice, ActionListener.wrap(handler::onStackTraceResponse, submitListener::onFailure));
        }

        if (hostIds.isEmpty()) {
            return;
        }

        // Retrieve the host metadata in parallel. Assume low-cardinality and do not split the query.
        client.prepareSearch("profiling-hosts")
            .setTrackTotalHits(false)
            .setQuery(
                QueryBuilders.boolQuery()
                    .filter(
                        // Only return hosts that have been active during the requested time period
                        QueryBuilders.rangeQuery("@timestamp")
                            // HAs write host metadata every 6h, so use start minus 6h.
                            .gte(responseBuilder.getStart().minus(Duration.ofHours(6L)).toEpochMilli())
                            .lt(responseBuilder.getEnd().toEpochMilli())
                            .format("epoch_millis")
                    )
                    .filter(QueryBuilders.termsQuery("host.id", hostIds))
            )
            .setCollapse(
                // Collapse on host.id to get a single host metadata for each host.
                new CollapseBuilder("host.id")
            )
            // Sort descending by timestamp to get the latest host metadata for each host.
            .addSort(new FieldSortBuilder("@timestamp").order(SortOrder.DESC))
            .setFrom(0)
            .execute(ActionListener.wrap(handler::onHostsResponse, submitListener::onFailure));
    }

    // package private for testing
    static <T> List<List<T>> sliced(List<T> c, int slices) {
        if (c.size() <= slices || slices == 1) {
            return List.of(c);
        }
        List<List<T>> slicedList = new ArrayList<>();
        int batchSize = c.size() / slices;
        for (int slice = 0; slice < slices; slice++) {
            int upperIndex = (slice + 1 < slices) ? (slice + 1) * batchSize : c.size();
            List<T> ids = c.subList(slice * batchSize, upperIndex);
            slicedList.add(ids);
        }
        return Collections.unmodifiableList(slicedList);
    }

    private class StackTraceHandler {
        private final AtomicInteger expectedResponses;
        private final CancellableTask submitTask;
        private final ClusterState clusterState;
        private final Client client;
        private final GetStackTracesResponseBuilder responseBuilder;
        private final ActionListener<GetStackTracesResponse> submitListener;
        private final Map<String, StackTrace> stackTracePerId;
        private final Set<String> stackFrameIds;
        private final Set<String> executableIds;
        private final AtomicInteger totalFrames = new AtomicInteger();
        private final StopWatch watch = new StopWatch("retrieveStackTraces");
        private final StopWatch hostsWatch = new StopWatch("retrieveHostMetadata");
        private final Map<String, HostMetadata> hostMetadata;

        private StackTraceHandler(
            CancellableTask submitTask,
            ClusterState clusterState,
            Client client,
            GetStackTracesResponseBuilder responseBuilder,
            ActionListener<GetStackTracesResponse> submitListener,
            int stackTraceCount,
            int expectedResponses,
            int expectedHosts
        ) {
            this.submitTask = submitTask;
            this.clusterState = clusterState;
            this.stackTracePerId = new ConcurrentHashMap<>(stackTraceCount);
            // pre-size with a bit of headroom so the collection isn't resized too often
            this.stackFrameIds = ConcurrentHashMap.newKeySet(stackTraceCount * 5);
            this.executableIds = ConcurrentHashMap.newKeySet(stackTraceCount);
            this.expectedResponses = new AtomicInteger(expectedResponses);
            this.client = client;
            this.responseBuilder = responseBuilder;
            this.submitListener = submitListener;
            this.hostMetadata = new HashMap<>(expectedHosts);
        }

        public void onStackTraceResponse(MultiGetResponse multiGetItemResponses) {
            for (MultiGetItemResponse trace : multiGetItemResponses) {
                if (trace.isFailed()) {
                    submitListener.onFailure(trace.getFailure().getFailure());
                    return;
                }
                if (trace.getResponse().isExists()) {
                    String id = trace.getId();
                    // Duplicates are expected as we query multiple indices - do a quick pre-check before we deserialize a response
                    if (stackTracePerId.containsKey(id) == false) {
                        StackTrace stacktrace = StackTrace.fromSource(trace.getResponse().getSource());
                        // Guard against concurrent access and ensure we only handle each item once
                        if (stackTracePerId.putIfAbsent(id, stacktrace) == null) {
                            totalFrames.addAndGet(stacktrace.frameIds.length);
                            stackFrameIds.addAll(List.of(stacktrace.frameIds));
                            stacktrace.forNativeAndKernelFrames(executableIds::add);
                        }
                    }
                }
            }
            mayFinish();
        }

        public void onHostsResponse(SearchResponse searchResponse) {
            SearchHit[] hits = searchResponse.getHits().getHits();
            for (SearchHit hit : hits) {
                HostMetadata host = HostMetadata.fromSource(hit.getSourceAsMap());
                hostMetadata.put(host.hostID, host);
            }
            log.debug(hostsWatch::report);
            log.debug("Got [{}] host metadata items", hostMetadata.size());

            mayFinish();
        }

        public void calculateCO2AndCosts() {
            // Do the CO2 and cost calculation in parallel to waiting for frame metadata.
            StopWatch watch = new StopWatch("calculateCO2AndCosts");
            CO2Calculator co2Calculator = new CO2Calculator(
                hostMetadata,
                responseBuilder.getRequestedDuration(),
                responseBuilder.getCustomCO2PerKWH(),
                responseBuilder.getCustomDatacenterPUE(),
                responseBuilder.getCustomPerCoreWattX86(),
                responseBuilder.getCustomPerCoreWattARM64()
            );
            CostCalculator costCalculator = new CostCalculator(
                hostMetadata,
                responseBuilder.getRequestedDuration(),
                responseBuilder.getAWSCostFactor(),
                responseBuilder.getAzureCostFactor(),
                responseBuilder.getCustomCostPerCoreHour()
            );

            responseBuilder.getStackTraceEvents().forEach((eventId, event) -> {
                event.annualCO2Tons += co2Calculator.getAnnualCO2Tons(eventId.hostID(), event.count, eventId.samplingFrequency());
                event.annualCostsUSD += costCalculator.annualCostsUSD(eventId.hostID(), event.count, eventId.samplingFrequency());
            });

            log.debug(watch::report);
        }

        public void mayFinish() {
            if (expectedResponses.decrementAndGet() == 0) {
                calculateCO2AndCosts();

                responseBuilder.setStackTraces(stackTracePerId);
                responseBuilder.setTotalFrames(totalFrames.get());
                log.debug(
                    "retrieveStackTraces found [{}] stack traces, [{}] frames, [{}] executables.",
                    stackTracePerId.size(),
                    stackFrameIds.size(),
                    executableIds.size()
                );
                log.debug(watch::report);
                retrieveStackTraceDetails(
                    submitTask,
                    clusterState,
                    client,
                    responseBuilder,
                    new ArrayList<>(stackFrameIds),
                    new ArrayList<>(executableIds),
                    submitListener
                );
            }
        }
    }

    private void retrieveStackTraceDetails(
        CancellableTask submitTask,
        ClusterState clusterState,
        Client client,
        GetStackTracesResponseBuilder responseBuilder,
        List<String> stackFrameIds,
        List<String> executableIds,
        ActionListener<GetStackTracesResponse> submitListener
    ) {
        if (submitTask.notifyIfCancelled(submitListener)) {
            return;
        }
        List<Index> stackFrameIndices = resolver.resolve(
            clusterState,
            "profiling-stackframes",
            responseBuilder.getStart(),
            responseBuilder.getEnd()
        );
        List<Index> executableIndices = resolver.resolve(
            clusterState,
            "profiling-executables",
            responseBuilder.getStart(),
            responseBuilder.getEnd()
        );
        // Avoid parallelism if there is potential we are on spinning disks (frozen tier uses searchable snapshots)
        int stackFrameSliceCount = IndexAllocation.isAnyOnWarmOrColdTier(clusterState, stackFrameIndices) ? 1 : desiredDetailSlices;
        int executableSliceCount = IndexAllocation.isAnyOnWarmOrColdTier(clusterState, executableIndices) ? 1 : desiredDetailSlices;
        log.trace(
            "Using [{}] slice(s) to lookup stack frames and [{}] slice(s) to lookup executables.",
            stackFrameSliceCount,
            executableSliceCount
        );

        List<List<String>> slicedStackFrameIds = sliced(stackFrameIds, stackFrameSliceCount);
        List<List<String>> slicedExecutableIds = sliced(executableIds, executableSliceCount);

        DetailsHandler handler = new DetailsHandler(
            responseBuilder,
            submitListener,
            executableIds.size(),
            stackFrameIds.size(),
            slicedExecutableIds.size() * executableIndices.size(),
            slicedStackFrameIds.size() * stackFrameIndices.size()
        );

        if (stackFrameIds.isEmpty()) {
            handler.onStackFramesResponse(new MultiGetResponse(new MultiGetItemResponse[0]));
        } else {
            for (List<String> slice : slicedStackFrameIds) {
                mget(client, stackFrameIndices, slice, ActionListener.wrap(handler::onStackFramesResponse, submitListener::onFailure));
            }
        }
        // no data dependency - we can do this concurrently
        if (executableIds.isEmpty()) {
            handler.onExecutableDetailsResponse(new MultiGetResponse(new MultiGetItemResponse[0]));
        } else {
            for (List<String> slice : slicedExecutableIds) {
                mget(
                    client,
                    executableIndices,
                    slice,
                    ActionListener.wrap(handler::onExecutableDetailsResponse, submitListener::onFailure)
                );
            }
        }
    }

    /**
     * Collects stack trace details which are retrieved concurrently and sends a response only when all details are known.
     */
    private static class DetailsHandler {
        private static final String[] PATH_FILE_NAME = new String[] { "Executable", "file", "name" };
        private final GetStackTracesResponseBuilder builder;
        private final ActionListener<GetStackTracesResponse> submitListener;
        private final Map<String, String> executables;
        private final Map<String, StackFrame> stackFrames;
        private final AtomicInteger expectedSlices;
        private final AtomicInteger totalInlineFrames = new AtomicInteger();
        private final StopWatch watch = new StopWatch("retrieveStackTraceDetails");

        private DetailsHandler(
            GetStackTracesResponseBuilder builder,
            ActionListener<GetStackTracesResponse> submitListener,
            int executableCount,
            int stackFrameCount,
            int expectedExecutableSlices,
            int expectedStackFrameSlices
        ) {
            this.builder = builder;
            this.submitListener = submitListener;
            this.executables = new ConcurrentHashMap<>(executableCount);
            this.stackFrames = new ConcurrentHashMap<>(stackFrameCount);
            // for deciding when we're finished it is irrelevant where a slice originated, so we can
            // simplify state handling by treating them equally.
            this.expectedSlices = new AtomicInteger(expectedExecutableSlices + expectedStackFrameSlices);
        }

        public void onStackFramesResponse(MultiGetResponse multiGetItemResponses) {
            for (MultiGetItemResponse frame : multiGetItemResponses) {
                if (frame.isFailed()) {
                    submitListener.onFailure(frame.getFailure().getFailure());
                    return;
                }
                if (frame.getResponse().isExists()) {
                    // Duplicates are expected as we query multiple indices - do a quick pre-check before we deserialize a response
                    if (stackFrames.containsKey(frame.getId()) == false) {
                        StackFrame stackFrame = StackFrame.fromSource(frame.getResponse().getSource());
                        if (stackFrame.isEmpty() == false) {
                            if (stackFrames.putIfAbsent(frame.getId(), stackFrame) == null) {
                                totalInlineFrames.addAndGet(stackFrame.inlineFrameCount());
                            }
                        } else {
                            log.trace("Stack frame with id [{}] has no properties.", frame.getId());
                        }
                    }
                }
            }
            mayFinish();
        }

        public void onExecutableDetailsResponse(MultiGetResponse multiGetItemResponses) {
            for (MultiGetItemResponse executable : multiGetItemResponses) {
                if (executable.isFailed()) {
                    submitListener.onFailure(executable.getFailure().getFailure());
                    return;
                }
                if (executable.getResponse().isExists()) {
                    // Duplicates are expected as we query multiple indices - do a quick pre-check before we deserialize a response
                    if (executables.containsKey(executable.getId()) == false) {
                        Map<String, Object> source = executable.getResponse().getSource();
                        String fileName = ObjectPath.eval(PATH_FILE_NAME, source);
                        if (fileName == null) {
                            // If synthetic source is disabled, read from dotted field names.
                            fileName = (String) source.get("Executable.file.name");
                        }
                        if (fileName != null) {
                            executables.putIfAbsent(executable.getId(), fileName);
                        } else {
                            String priorKey = executables.putIfAbsent(executable.getId(), "<missing>");
                            // avoid spurious logging by checking whether we have actually inserted the 'missing' key
                            if (priorKey == null) {
                                log.trace("Executable with id [{}] has no file name.", executable.getId());
                            }
                        }
                    }
                }
            }
            mayFinish();
        }

        public void mayFinish() {
            if (expectedSlices.decrementAndGet() == 0) {
                builder.setExecutables(executables);
                builder.setStackFrames(stackFrames);
                builder.addTotalFrames(totalInlineFrames.get());
                log.debug("retrieveStackTraceDetails found [{}] stack frames, [{}] executables.", stackFrames.size(), executables.size());
                log.debug(watch::report);
                submitListener.onResponse(builder.build());
            }
        }
    }

    private void mget(Client client, List<Index> indices, List<String> slice, ActionListener<MultiGetResponse> listener) {
        for (Index index : indices) {
            client.prepareMultiGet()
                .addIds(index.getName(), slice)
                .setRealtime(realtime)
                .execute(new RefCountAwareThreadedActionListener<>(responseExecutor, listener));
        }
    }
}
