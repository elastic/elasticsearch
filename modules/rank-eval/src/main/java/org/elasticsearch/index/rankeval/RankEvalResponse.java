/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Returns the results for a {@link RankEvalRequest}.<br>
 * The response contains a detailed section for each evaluation query in the request and
 * possible failures that happened when execution individual queries.
 **/
public class RankEvalResponse extends ActionResponse implements ChunkedToXContentObject {

    /** The overall evaluation result. */
    private double metricScore;
    /** details about individual ranking evaluation queries, keyed by their id */
    private Map<String, EvalQueryQuality> details;
    /** exceptions for specific ranking evaluation queries, keyed by their id */
    private Map<String, Exception> failures;

    public RankEvalResponse(double metricScore, Map<String, EvalQueryQuality> partialResults, Map<String, Exception> failures) {
        this.metricScore = metricScore;
        this.details = new HashMap<>(partialResults);
        this.failures = new HashMap<>(failures);
    }

    RankEvalResponse(StreamInput in) throws IOException {
        super(in);
        this.metricScore = in.readDouble();
        int partialResultSize = in.readVInt();
        this.details = Maps.newMapWithExpectedSize(partialResultSize);
        for (int i = 0; i < partialResultSize; i++) {
            String queryId = in.readString();
            EvalQueryQuality partial = new EvalQueryQuality(in);
            this.details.put(queryId, partial);
        }
        int failuresSize = in.readVInt();
        this.failures = Maps.newMapWithExpectedSize(failuresSize);
        for (int i = 0; i < failuresSize; i++) {
            String queryId = in.readString();
            this.failures.put(queryId, in.readException());
        }
    }

    public double getMetricScore() {
        return metricScore;
    }

    public Map<String, EvalQueryQuality> getPartialResults() {
        return Collections.unmodifiableMap(details);
    }

    public Map<String, Exception> getFailures() {
        return Collections.unmodifiableMap(failures);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(metricScore);
        out.writeMap(details, StreamOutput::writeWriteable);
        out.writeMap(failures, StreamOutput::writeException);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {
        return Iterators.concat(
            ChunkedToXContentHelper.singleChunk(
                (builder, params) -> builder.startObject().field("metric_score", metricScore).startObject("details")
            ),
            Iterators.flatMap(details.keySet().iterator(), key -> details.get(key).toXContentChunked(outerParams)),
            ChunkedToXContentHelper.singleChunk((builder, params) -> builder.endObject().startObject("failures")),
            Iterators.flatMap(failures.keySet().iterator(), key -> ChunkedToXContentHelper.singleChunk((builder, params) -> {
                builder.startObject(key);
                ElasticsearchException.generateFailureXContent(builder, params, failures.get(key), true);
                builder.endObject();
                return builder;
            })),
            ChunkedToXContentHelper.singleChunk((builder, params) -> builder.endObject().endObject())
        );
    }
}
