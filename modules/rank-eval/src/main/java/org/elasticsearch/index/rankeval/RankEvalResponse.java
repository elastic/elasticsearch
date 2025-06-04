/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Returns the results for a {@link RankEvalRequest}.<br>
 * The response contains a detailed section for each evaluation query in the request and
 * possible failures that happened when execution individual queries.
 **/
public class RankEvalResponse extends ActionResponse implements ToXContentObject {

    /** The overall evaluation result. */
    private final double metricScore;
    /** details about individual ranking evaluation queries, keyed by their id */
    private final Map<String, EvalQueryQuality> details;
    /** exceptions for specific ranking evaluation queries, keyed by their id */
    private final Map<String, Exception> failures;

    public RankEvalResponse(double metricScore, Map<String, EvalQueryQuality> partialResults, Map<String, Exception> failures) {
        this.metricScore = metricScore;
        this.details = new HashMap<>(partialResults);
        this.failures = new HashMap<>(failures);
    }

    RankEvalResponse(StreamInput in) throws IOException {
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
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("metric_score", metricScore);
        builder.startObject("details");
        for (String key : details.keySet()) {
            details.get(key).toXContent(builder, params);
        }
        builder.endObject();
        builder.startObject("failures");
        for (String key : failures.keySet()) {
            builder.startObject(key);
            ElasticsearchException.generateFailureXContent(builder, params, failures.get(key), true);
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }
}
