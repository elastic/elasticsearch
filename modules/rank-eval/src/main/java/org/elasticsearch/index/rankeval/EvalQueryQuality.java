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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.rankeval.RatedDocument.DocumentKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;;

/**
 * Result of the evaluation metric calculation on one particular query alone.
 */
public class EvalQueryQuality implements ToXContent, Writeable {

    private final String queryId;
    private final double evaluationResult;
    private MetricDetails optionalMetricDetails;
    private final List<RatedSearchHit> ratedHits = new ArrayList<>();

    public EvalQueryQuality(String id, double evaluationResult) {
        this.queryId = id;
        this.evaluationResult = evaluationResult;
    }

    public EvalQueryQuality(StreamInput in) throws IOException {
        this(in.readString(), in.readDouble());
        this.ratedHits.addAll(in.readList(RatedSearchHit::new));
        this.optionalMetricDetails = in.readOptionalNamedWriteable(MetricDetails.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(queryId);
        out.writeDouble(evaluationResult);
        out.writeList(ratedHits);
        out.writeOptionalNamedWriteable(this.optionalMetricDetails);
    }

    public String getId() {
        return queryId;
    }

    public double getQualityLevel() {
        return evaluationResult;
    }

    public void setMetricDetails(MetricDetails breakdown) {
        this.optionalMetricDetails = breakdown;
    }

    public MetricDetails getMetricDetails() {
        return this.optionalMetricDetails;
    }

    public void addHitsAndRatings(List<RatedSearchHit> hits) {
        this.ratedHits.addAll(hits);
    }

    public List<RatedSearchHit> getHitsAndRatings() {
        return this.ratedHits;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(queryId);
        builder.field("quality_level", this.evaluationResult);
        builder.startArray("unknown_docs");
        for (DocumentKey key : EvaluationMetric.filterUnknownDocuments(ratedHits)) {
            builder.startObject();
            builder.field(RatedDocument.INDEX_FIELD.getPreferredName(), key.getIndex());
            builder.field(RatedDocument.DOC_ID_FIELD.getPreferredName(), key.getDocId());
            builder.endObject();
        }
        builder.endArray();
        builder.startArray("hits");
        for (RatedSearchHit hit : ratedHits) {
            hit.toXContent(builder, params);
        }
        builder.endArray();
        if (optionalMetricDetails != null) {
            builder.startObject("metric_details");
            optionalMetricDetails.toXContent(builder, params);
            builder.endObject();
        }
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
        EvalQueryQuality other = (EvalQueryQuality) obj;
        return Objects.equals(queryId, other.queryId) &&
                Objects.equals(evaluationResult, other.evaluationResult) &&
                Objects.equals(ratedHits, other.ratedHits) &&
                Objects.equals(optionalMetricDetails, other.optionalMetricDetails);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(queryId, evaluationResult, ratedHits, optionalMetricDetails);
    }
}
