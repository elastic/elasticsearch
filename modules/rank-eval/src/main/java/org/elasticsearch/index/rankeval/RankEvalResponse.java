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

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * For each qa specification identified by its id this response returns the respective
 * averaged precisionAnN value.
 *
 * In addition for each query the document ids that haven't been found annotated is returned as well.
 *
 * Documents of unknown quality - i.e. those that haven't been supplied in the set of annotated documents but have been returned
 * by the search are not taken into consideration when computing precision at n - they are ignored.
 *
 **/
//TODO instead of just returning averages over complete results, think of other statistics, micro avg, macro avg, partial results
public class RankEvalResponse extends ActionResponse implements ToXContent {
    /**Average precision observed when issuing query intents with this specification.*/
    private double qualityLevel;
    /**Mapping from intent id to all documents seen for this intent that were not annotated.*/
    private Map<String, EvalQueryQuality> details;

    public RankEvalResponse() {
    }

    public RankEvalResponse(double qualityLevel, Map<String, EvalQueryQuality> partialResults) {
        this.qualityLevel = qualityLevel;
        this.details = partialResults;
    }

    public double getQualityLevel() {
        return qualityLevel;
    }

    public Map<String, EvalQueryQuality> getPartialResults() {
        return Collections.unmodifiableMap(details);
    }

    @Override
    public String toString() {
        return "RankEvalResponse, quality: " + qualityLevel + ", partial results: " + details;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeDouble(qualityLevel);
        out.writeVInt(details.size());
        for (String queryId : details.keySet()) {
            out.writeString(queryId);
            details.get(queryId).writeTo(out);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.qualityLevel = in.readDouble();
        int partialResultSize = in.readVInt();
        this.details = new HashMap<>(partialResultSize);
        for (int i = 0; i < partialResultSize; i++) {
            String queryId = in.readString();
            EvalQueryQuality partial = new EvalQueryQuality(in);
            this.details.put(queryId, partial);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("rank_eval");
        builder.field("quality_level", qualityLevel);
        builder.startObject("details");
        for (String key : details.keySet()) {
            details.get(key).toXContent(builder, params);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RankEvalResponse other = (RankEvalResponse) obj;
        return Objects.equals(qualityLevel, other.qualityLevel) &&
                Objects.equals(details, other.details);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(qualityLevel, details);
    }
}
