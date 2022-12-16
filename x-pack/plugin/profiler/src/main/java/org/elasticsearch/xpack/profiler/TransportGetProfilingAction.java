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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ObjectPath;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class TransportGetProfilingAction extends HandledTransportAction<GetProfilingRequest, GetProfilingResponse> {
    private static final Logger log = LogManager.getLogger(TransportGetProfilingAction.class);
    private final NodeClient nodeClient;
    private final TransportService transportService;

    @Inject
    public TransportGetProfilingAction(TransportService transportService, ActionFilters actionFilters, NodeClient nodeClient) {
        super(GetProfilingAction.NAME, transportService, actionFilters, GetProfilingRequest::new);
        this.nodeClient = nodeClient;
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(Task submitTask, GetProfilingRequest request, ActionListener<GetProfilingResponse> submitListener) {
        Client client = new ParentTaskAssigningClient(this.nodeClient, transportService.getLocalNode(), submitTask);
        EventsIndex mediumDownsampled = EventsIndex.MEDIUM_DOWNSAMPLED;
        client.prepareSearch(mediumDownsampled.getName())
            .setSize(0)
            .setQuery(request.getQuery())
            .setTrackTotalHits(true)
            .execute(new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    long sampleCount = searchResponse.getHits().getTotalHits().value;
                    EventsIndex resampledIndex = mediumDownsampled.getResampledIndex(request.getSampleSize(), sampleCount);
                    searchEventGroupByStackTrace(client, request, resampledIndex, submitListener);
                }

                @Override
                public void onFailure(Exception e) {
                    // Apart from profiling-events-all, indices are created lazily. In a relatively empty cluster it can happen
                    // that there are so few data that we need to resort to the full index. As this is an edge case we'd rather
                    // fail instead of prematurely checking for existence in all cases.
                    if (e instanceof IndexNotFoundException) {
                        String missingIndex = ((IndexNotFoundException) e).getIndex().getName();
                        EventsIndex fullIndex = EventsIndex.FULL_INDEX;
                        log.debug("Index [{}] does not exist. Using [{}] instead.", missingIndex, fullIndex.getName());
                        searchEventGroupByStackTrace(client, request, fullIndex, submitListener);
                    } else {
                        submitListener.onFailure(e);
                    }
                }
            });
    }

    private void searchEventGroupByStackTrace(
        Client client,
        GetProfilingRequest request,
        EventsIndex eventsIndex,
        ActionListener<GetProfilingResponse> submitListener
    ) {
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
            .execute(new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
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
                    if (stackTraceEvents.isEmpty() == false) {
                        responseBuilder.setStackTraceEvents(stackTraceEvents);
                        retrieveStackTraces(client, responseBuilder, submitListener);
                    } else {
                        submitListener.onResponse(responseBuilder.build());
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    submitListener.onFailure(e);
                }
            });
    }

    private void retrieveStackTraces(
        Client client,
        GetProfilingResponseBuilder responseBuilder,
        ActionListener<GetProfilingResponse> submitListener
    ) {
        client.prepareMultiGet()
            .addIds("profiling-stacktraces", responseBuilder.getStackTraceEvents().keySet())
            .setRealtime(true)
            .execute(new ActionListener<>() {
                @Override
                public void onResponse(MultiGetResponse multiGetItemResponses) {
                    Map<String, StackTrace> stackTracePerId = new HashMap<>();
                    // sort items lexicographically to access Lucene's term dictionary more efficiently when issuing an mget request.
                    // The term dictionary is lexicographically sorted and using the same order reduces the number of page faults
                    // needed to load it.
                    Set<String> stackFrameIds = new TreeSet<>();
                    Set<String> executableIds = new TreeSet<>();
                    int totalFrames = 0;
                    for (MultiGetItemResponse trace : multiGetItemResponses) {
                        if (trace.isFailed() == false && trace.getResponse().isExists()) {
                            String id = trace.getId();
                            StackTrace stacktrace = StackTrace.fromSource(trace.getResponse().getSource());
                            stackTracePerId.put(id, stacktrace);
                            totalFrames += stacktrace.frameIds.length;
                            stackFrameIds.addAll(Arrays.asList(stacktrace.frameIds));
                            executableIds.addAll(Arrays.asList(stacktrace.fileIds));
                        }
                    }
                    responseBuilder.setStackTraces(stackTracePerId);
                    responseBuilder.setTotalFrames(totalFrames);
                    retrieveStackTraceDetails(client, responseBuilder, stackFrameIds, executableIds, submitListener);
                }

                @Override
                public void onFailure(Exception e) {
                    submitListener.onFailure(e);
                }
            });
    }

    private void retrieveStackTraceDetails(
        Client client,
        GetProfilingResponseBuilder responseBuilder,
        Set<String> stackFrameIds,
        Set<String> executableIds,
        ActionListener<GetProfilingResponse> submitListener
    ) {

        DetailsHandler handler = new DetailsHandler(responseBuilder, submitListener);

        if (stackFrameIds.isEmpty()) {
            handler.onStackFramesResponse(new MultiGetResponse(new MultiGetItemResponse[0]));
        } else {
            client.prepareMultiGet().addIds("profiling-stackframes", stackFrameIds).setRealtime(true).execute(new ActionListener<>() {
                @Override
                public void onResponse(MultiGetResponse multiGetItemResponses) {
                    handler.onStackFramesResponse(multiGetItemResponses);
                }

                @Override
                public void onFailure(Exception e) {
                    submitListener.onFailure(e);
                }
            });
        }
        // no data dependency - we can do this concurrently
        if (executableIds.isEmpty()) {
            handler.onExecutableDetailsResponse(new MultiGetResponse(new MultiGetItemResponse[0]));
        } else {
            client.prepareMultiGet().addIds("profiling-executables", executableIds).setRealtime(true).execute(new ActionListener<>() {
                @Override
                public void onResponse(MultiGetResponse multiGetItemResponses) {
                    handler.onExecutableDetailsResponse(multiGetItemResponses);
                }

                @Override
                public void onFailure(Exception e) {
                    submitListener.onFailure(e);
                }
            });
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
        private volatile Map<String, String> executables;
        private volatile Map<String, StackFrame> stackFrames;

        private DetailsHandler(GetProfilingResponseBuilder builder, ActionListener<GetProfilingResponse> submitListener) {
            this.builder = builder;
            this.submitListener = submitListener;
        }

        public void onStackFramesResponse(MultiGetResponse multiGetItemResponses) {
            Map<String, StackFrame> stackFrames = new HashMap<>();
            for (MultiGetItemResponse frame : multiGetItemResponses) {
                if (frame.isFailed() == false && frame.getResponse().isExists()) {
                    stackFrames.put(frame.getId(), StackFrame.fromSource(frame.getResponse().getSource()));
                }
            }
            // publish to object state only when completely done, otherwise mayFinish() could run twice
            this.stackFrames = stackFrames;
            mayFinish();
        }

        public void onExecutableDetailsResponse(MultiGetResponse multiGetItemResponses) {
            Map<String, String> executables = new HashMap<>();
            for (MultiGetItemResponse executable : multiGetItemResponses) {
                if (executable.isFailed() == false && executable.getResponse().isExists()) {
                    executables.put(executable.getId(), ObjectPath.eval("Executable.file.name", executable.getResponse().getSource()));
                }
            }
            // publish to object state only when completely done, otherwise mayFinish() could run twice
            this.executables = executables;
            mayFinish();
        }

        public void mayFinish() {
            if (executables != null && stackFrames != null) {
                builder.setExecutables(executables);
                builder.setStackFrames(stackFrames);
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
