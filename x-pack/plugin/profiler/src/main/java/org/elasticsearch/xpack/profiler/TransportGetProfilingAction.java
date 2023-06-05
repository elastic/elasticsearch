/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.profiler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ObjectPath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

public class TransportGetProfilingAction extends HandledTransportAction<GetProfilingRequest, GetProfilingResponse> {
    private static final Logger log = LogManager.getLogger(TransportGetProfilingAction.class);

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

    private final NodeClient nodeClient;
    private final TransportService transportService;
    private final Executor responseExecutor;
    private final int desiredSlices;
    private final int desiredDetailSlices;
    private final boolean realtime;

    @Inject
    public TransportGetProfilingAction(
        Settings settings,
        ThreadPool threadPool,
        TransportService transportService,
        ActionFilters actionFilters,
        NodeClient nodeClient
    ) {
        super(GetProfilingAction.NAME, transportService, actionFilters, GetProfilingRequest::new);
        this.nodeClient = nodeClient;
        this.transportService = transportService;
        this.responseExecutor = threadPool.executor(ProfilingPlugin.PROFILING_THREAD_POOL_NAME);
        this.desiredSlices = PROFILING_MAX_STACKTRACE_QUERY_SLICES.get(settings);
        this.desiredDetailSlices = PROFILING_MAX_DETAIL_QUERY_SLICES.get(settings);
        this.realtime = PROFILING_QUERY_REALTIME.get(settings);
    }

    @Override
    protected void doExecute(Task submitTask, GetProfilingRequest request, ActionListener<GetProfilingResponse> submitListener) {
        long start = System.nanoTime();
        Client client = new ParentTaskAssigningClient(this.nodeClient, transportService.getLocalNode(), submitTask);
        EventsIndex mediumDownsampled = EventsIndex.MEDIUM_DOWNSAMPLED;
        client.prepareSearch(mediumDownsampled.getName())
            .setSize(0)
            .setQuery(request.getQuery())
            .setTrackTotalHits(true)
            .execute(ActionListener.wrap(searchResponse -> {
                long sampleCount = searchResponse.getHits().getTotalHits().value;
                EventsIndex resampledIndex = mediumDownsampled.getResampledIndex(request.getSampleSize(), sampleCount);
                log.debug("getResampledIndex took [" + (System.nanoTime() - start) / 1_000_000.0d + " ms].");
                searchEventGroupByStackTrace(client, request, resampledIndex, submitListener);
            }, e -> {
                // All profiling-events data streams are created lazily. In a relatively empty cluster it can happen that there are so few
                // data that we need to resort to the "full" events stream. As this is an edge case we'd rather fail instead of prematurely
                // checking for existence in all cases.
                if (e instanceof IndexNotFoundException) {
                    String missingIndex = ((IndexNotFoundException) e).getIndex().getName();
                    EventsIndex fullIndex = EventsIndex.FULL_INDEX;
                    log.debug("Index [{}] does not exist. Using [{}] instead.", missingIndex, fullIndex.getName());
                    searchEventGroupByStackTrace(client, request, fullIndex, submitListener);
                } else {
                    submitListener.onFailure(e);
                }
            }));
    }

    private void searchEventGroupByStackTrace(
        Client client,
        GetProfilingRequest request,
        EventsIndex eventsIndex,
        ActionListener<GetProfilingResponse> submitListener
    ) {
        long start = System.nanoTime();
        GetProfilingResponseBuilder responseBuilder = new GetProfilingResponseBuilder();
        client.prepareSearch(eventsIndex.getName())
            .setTrackTotalHits(false)
            .setQuery(request.getQuery())
            .addAggregation(
                new TermsAggregationBuilder("group_by")
                    // 'size' should be max 100k, but might be slightly more. Better be on the safe side.
                    .size(150_000)
                    .field("Stacktrace.id")
                    // 'execution_hint: map' skips the slow building of ordinals that we don't need.
                    // Especially with high cardinality fields, this makes aggregations really slow.
                    .executionHint("map")
                    .subAggregation(new SumAggregationBuilder("count").field("Stacktrace.count"))
            )
            .addAggregation(new SumAggregationBuilder("total_count").field("Stacktrace.count"))
            .execute(ActionListener.wrap(searchResponse -> {
                Sum totalCountAgg = searchResponse.getAggregations().get("total_count");
                long totalCount = Math.round(totalCountAgg.value());
                Resampler resampler = new Resampler(request, eventsIndex.getSampleRate(), totalCount);
                StringTerms stacktraces = searchResponse.getAggregations().get("group_by");
                // sort items lexicographically to access Lucene's term dictionary more efficiently when issuing an mget request.
                // The term dictionary is lexicographically sorted and using the same order reduces the number of page faults
                // needed to load it.
                Map<String, Integer> stackTraceEvents = new TreeMap<>();
                for (StringTerms.Bucket bucket : stacktraces.getBuckets()) {
                    Sum count = bucket.getAggregations().get("count");
                    int finalCount = resampler.adjustSampleCount((int) count.value());
                    if (finalCount > 0) {
                        stackTraceEvents.put(bucket.getKeyAsString(), finalCount);
                    }
                }
                log.debug("searchEventGroupByStackTrace took [" + (System.nanoTime() - start) / 1_000_000.0d + " ms].");
                if (stackTraceEvents.isEmpty() == false) {
                    responseBuilder.setStackTraceEvents(stackTraceEvents);
                    retrieveStackTraces(client, responseBuilder, submitListener);
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
            }));
    }

    private void retrieveStackTraces(
        Client client,
        GetProfilingResponseBuilder responseBuilder,
        ActionListener<GetProfilingResponse> submitListener
    ) {
        List<String> eventIds = new ArrayList<>(responseBuilder.getStackTraceEvents().keySet());
        List<List<String>> slicedEventIds = sliced(eventIds, desiredSlices);
        StackTraceHandler handler = new StackTraceHandler(client, responseBuilder, submitListener, eventIds.size(), slicedEventIds.size());
        for (List<String> slice : slicedEventIds) {
            client.prepareMultiGet()
                .setRealtime(realtime)
                .addIds("profiling-stacktraces", slice)
                .execute(
                    new ThreadedActionListener<>(responseExecutor, ActionListener.wrap(handler::onResponse, submitListener::onFailure))
                );
        }
    }

    // package private for testing
    static <T> List<List<T>> sliced(List<T> c, int slices) {
        if (c.size() <= slices) {
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
        private final AtomicInteger remainingSlices;
        private final Client client;
        private final GetProfilingResponseBuilder responseBuilder;
        private final ActionListener<GetProfilingResponse> submitListener;
        private final Map<String, StackTrace> stackTracePerId;
        // sort items lexicographically to access Lucene's term dictionary more efficiently when issuing an mget request.
        // The term dictionary is lexicographically sorted and using the same order reduces the number of page faults
        // needed to load it.
        private final Set<String> stackFrameIds = new ConcurrentSkipListSet<>();
        private final Set<String> executableIds = new ConcurrentSkipListSet<>();
        private final AtomicInteger totalFrames = new AtomicInteger();
        private final long start = System.nanoTime();

        private StackTraceHandler(
            Client client,
            GetProfilingResponseBuilder responseBuilder,
            ActionListener<GetProfilingResponse> submitListener,
            int stackTraceCount,
            int slices
        ) {
            this.stackTracePerId = new ConcurrentHashMap<>(stackTraceCount);
            this.remainingSlices = new AtomicInteger(slices);
            this.client = client;
            this.responseBuilder = responseBuilder;
            this.submitListener = submitListener;
        }

        public void onResponse(MultiGetResponse multiGetItemResponses) {
            for (MultiGetItemResponse trace : multiGetItemResponses) {
                if (trace.isFailed() == false && trace.getResponse().isExists()) {
                    String id = trace.getId();
                    StackTrace stacktrace = StackTrace.fromSource(trace.getResponse().getSource());
                    stackTracePerId.put(id, stacktrace);
                    totalFrames.addAndGet(stacktrace.frameIds.size());
                    stackFrameIds.addAll(stacktrace.frameIds);
                    executableIds.addAll(stacktrace.fileIds);
                }
            }
            if (this.remainingSlices.decrementAndGet() == 0) {
                responseBuilder.setStackTraces(stackTracePerId);
                responseBuilder.setTotalFrames(totalFrames.get());
                log.debug("retrieveStackTraces took [" + (System.nanoTime() - start) / 1_000_000.0d + " ms].");
                retrieveStackTraceDetails(
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
        Client client,
        GetProfilingResponseBuilder responseBuilder,
        List<String> stackFrameIds,
        List<String> executableIds,
        ActionListener<GetProfilingResponse> submitListener
    ) {
        List<List<String>> slicedStackFrameIds = sliced(stackFrameIds, desiredDetailSlices);
        List<List<String>> slicedExecutableIds = sliced(executableIds, desiredDetailSlices);
        DetailsHandler handler = new DetailsHandler(
            responseBuilder,
            submitListener,
            executableIds.size(),
            stackFrameIds.size(),
            slicedExecutableIds.size(),
            slicedStackFrameIds.size()
        );

        if (stackFrameIds.isEmpty()) {
            handler.onStackFramesResponse(new MultiGetResponse(new MultiGetItemResponse[0]));
        } else {
            for (List<String> slice : slicedStackFrameIds) {
                client.prepareMultiGet()
                    .addIds("profiling-stackframes", slice)
                    .setRealtime(realtime)
                    .execute(
                        new ThreadedActionListener<>(
                            responseExecutor,
                            ActionListener.wrap(handler::onStackFramesResponse, submitListener::onFailure)
                        )
                    );
            }
        }
        // no data dependency - we can do this concurrently
        if (executableIds.isEmpty()) {
            handler.onExecutableDetailsResponse(new MultiGetResponse(new MultiGetItemResponse[0]));
        } else {
            for (List<String> slice : slicedExecutableIds) {
                client.prepareMultiGet()
                    .addIds("profiling-executables", slice)
                    .setRealtime(realtime)
                    .execute(
                        new ThreadedActionListener<>(
                            responseExecutor,
                            ActionListener.wrap(handler::onExecutableDetailsResponse, submitListener::onFailure)
                        )
                    );
            }
        }
    }

    private static class Resampler {
        private final boolean requiresResampling;

        private final Random r;

        private final double sampleRate;

        private final double p;

        Resampler(GetProfilingRequest request, double sampleRate, long totalCount) {
            // Manually reduce sample count if totalCount exceeds sampleSize by 10%.
            if (totalCount > request.getSampleSize() * 1.1) {
                this.requiresResampling = true;
                // Make the RNG predictable to get reproducible results.
                this.r = new Random(request.hashCode());
                this.sampleRate = sampleRate;
                this.p = (double) request.getSampleSize() / totalCount;
            } else {
                this.requiresResampling = false;
                this.r = null;
                this.sampleRate = sampleRate;
                this.p = 1.0d;
            }
        }

        public int adjustSampleCount(int originalCount) {
            if (requiresResampling) {
                int newCount = 0;
                for (int i = 0; i < originalCount; i++) {
                    if (r.nextDouble() < p) {
                        newCount++;
                    }
                }
                if (newCount > 0) {
                    // Adjust the sample counts from down-sampled to fully sampled.
                    // Be aware that downsampling drops entries from stackTraceEvents, so that
                    // the sum of the upscaled count values is less that totalCount.
                    return (int) Math.floor(newCount / (sampleRate * p));
                } else {
                    return 0;
                }
            } else {
                return originalCount;
            }
        }
    }

    /**
     * Collects stack trace details which are retrieved concurrently and sends a response only when all details are known.
     */
    private static class DetailsHandler {
        private final GetProfilingResponseBuilder builder;
        private final ActionListener<GetProfilingResponse> submitListener;
        private final Map<String, String> executables;
        private final Map<String, StackFrame> stackFrames;
        private final AtomicInteger expectedSlices;
        private final long start = System.nanoTime();

        private DetailsHandler(
            GetProfilingResponseBuilder builder,
            ActionListener<GetProfilingResponse> submitListener,
            int executableCount,
            int stackFrameCount,
            int expectedExecutableSlices,
            int expectedStackFrameSlices
        ) {
            this.builder = builder;
            this.submitListener = submitListener;
            this.executables = new ConcurrentHashMap<>(executableCount);
            this.stackFrames = new ConcurrentHashMap<>(stackFrameCount);
            // for deciding when we're finished it is irrelevant where a slice originated so we can
            // simplify state handling by treating them equally.
            this.expectedSlices = new AtomicInteger(expectedExecutableSlices + expectedStackFrameSlices);
        }

        public void onStackFramesResponse(MultiGetResponse multiGetItemResponses) {
            for (MultiGetItemResponse frame : multiGetItemResponses) {
                if (frame.isFailed() == false && frame.getResponse().isExists()) {
                    stackFrames.put(frame.getId(), StackFrame.fromSource(frame.getResponse().getSource()));
                }
            }
            mayFinish();
        }

        public void onExecutableDetailsResponse(MultiGetResponse multiGetItemResponses) {
            for (MultiGetItemResponse executable : multiGetItemResponses) {
                if (executable.isFailed() == false && executable.getResponse().isExists()) {
                    executables.put(executable.getId(), ObjectPath.eval("Executable.file.name", executable.getResponse().getSource()));
                }
            }
            mayFinish();
        }

        public void mayFinish() {
            if (expectedSlices.decrementAndGet() == 0) {
                builder.setExecutables(executables);
                builder.setStackFrames(stackFrames);
                log.debug("retrieveStackTraceDetails took [" + (System.nanoTime() - start) / 1_000_000.0d + " ms].");
                submitListener.onResponse(builder.build());
            }
        }
    }

    private static class GetProfilingResponseBuilder {
        private Map<String, StackTrace> stackTraces;
        private int totalFrames;
        private Map<String, StackFrame> stackFrames;
        private Map<String, String> executables;
        private Map<String, Integer> stackTraceEvents;
        private Exception error;

        public void setStackTraces(Map<String, StackTrace> stackTraces) {
            this.stackTraces = stackTraces;
        }

        public void setTotalFrames(int totalFrames) {
            this.totalFrames = totalFrames;
        }

        public void setStackFrames(Map<String, StackFrame> stackFrames) {
            this.stackFrames = stackFrames;
        }

        public void setExecutables(Map<String, String> executables) {
            this.executables = executables;
        }

        public void setStackTraceEvents(Map<String, Integer> stackTraceEvents) {
            this.stackTraceEvents = stackTraceEvents;
        }

        public Map<String, Integer> getStackTraceEvents() {
            return stackTraceEvents;
        }

        public void setError(Exception error) {
            this.error = error;
        }

        public GetProfilingResponse build() {
            if (error != null) {
                return new GetProfilingResponse(error);
            } else {
                return new GetProfilingResponse(stackTraces, stackFrames, executables, stackTraceEvents, totalFrames);
            }
        }
    }
}
