/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Returns the results for a {@link RankEvalRequest}.<br>
 * The repsonse contains a detailed section for each evaluation query in the request and
 * possible failures that happened when executin individual queries.
 **/
public class RankEvalResponse extends ActionResponse implements ToXContentObject {

    /** The overall evaluation result. */
    private double evaluationResult;
    /** details about individual ranking evaluation queries, keyed by their id */
    private Map<String, EvalQueryQuality> details;
    /** exceptions for specific ranking evaluation queries, keyed by their id */
    private Map<String, Exception> failures;

    public RankEvalResponse(double qualityLevel, Map<String, EvalQueryQuality> partialResults,
            Map<String, Exception> failures) {
        this.evaluationResult = qualityLevel;
        this.details =  new HashMap<>(partialResults);
        this.failures = new HashMap<>(failures);
    }

    RankEvalResponse() {
        // only used in RankEvalAction#newResponse()
    }

    public double getEvaluationResult() {
        return evaluationResult;
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
        super.writeTo(out);
        out.writeDouble(evaluationResult);
        out.writeVInt(details.size());
        for (String queryId : details.keySet()) {
            out.writeString(queryId);
            details.get(queryId).writeTo(out);
        }
        out.writeVInt(failures.size());
        for (String queryId : failures.keySet()) {
            out.writeString(queryId);
            out.writeException(failures.get(queryId));
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.evaluationResult = in.readDouble();
        int partialResultSize = in.readVInt();
        this.details = new HashMap<>(partialResultSize);
        for (int i = 0; i < partialResultSize; i++) {
            String queryId = in.readString();
            EvalQueryQuality partial = new EvalQueryQuality(in);
            this.details.put(queryId, partial);
        }
        int failuresSize = in.readVInt();
        this.failures = new HashMap<>(failuresSize);
        for (int i = 0; i < failuresSize; i++) {
            String queryId = in.readString();
            this.failures.put(queryId, in.readException());
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("quality_level", evaluationResult);
        builder.startObject("details");
        for (String key : details.keySet()) {
            details.get(key).toXContent(builder, params);
        }
        builder.endObject();
        builder.startObject("failures");
        for (String key : failures.keySet()) {
            builder.startObject(key);
            ElasticsearchException.generateFailureXContent(builder, params, failures.get(key), false);
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }
}
