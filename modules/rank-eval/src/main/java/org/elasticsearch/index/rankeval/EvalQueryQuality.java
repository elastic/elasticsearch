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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;;

/**
 * This class represents the partial information from running the ranking evaluation metric on one
 * request alone. It contains all information necessary to render the response for this part of the
 * overall evaluation.
 */
public class EvalQueryQuality implements ToXContent, Writeable {

    /** documents seen as result for one request that were not annotated.*/
    private List<DocumentKey> unknownDocs = new ArrayList<>();
    private String id;
    private double qualityLevel;
    private MetricDetails optionalMetricDetails;
    private List<RatedSearchHit> hits = new ArrayList<>();

    public EvalQueryQuality(String id, double qualityLevel) {
        this.id = id;
        this.qualityLevel = qualityLevel;
    }

    public EvalQueryQuality(StreamInput in) throws IOException {
        this(in.readString(), in.readDouble());
        this.unknownDocs = in.readList(DocumentKey::new);
        this.hits = in.readList(RatedSearchHit::new);
        this.optionalMetricDetails = in.readOptionalNamedWriteable(MetricDetails.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeDouble(qualityLevel);
        out.writeList(unknownDocs);
        out.writeList(hits);
        out.writeOptionalNamedWriteable(this.optionalMetricDetails);
    }

    public String getId() {
        return id;
    }

    public double getQualityLevel() {
        return qualityLevel;
    }

    public void setUnknownDocs(List<DocumentKey> unknownDocs) {
        this.unknownDocs = unknownDocs;
    }

    public List<DocumentKey> getUnknownDocs() {
        return Collections.unmodifiableList(this.unknownDocs);
    }

    public void addMetricDetails(MetricDetails breakdown) {
        this.optionalMetricDetails = breakdown;
    }

    public MetricDetails getMetricDetails() {
        return this.optionalMetricDetails;
    }

    public void addHitsAndRatings(List<RatedSearchHit> hits) {
        this.hits = hits;
    }

    public List<RatedSearchHit> getHitsAndRatings() {
        return this.hits;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(id);
        builder.field("quality_level", this.qualityLevel);
        builder.startArray("unknown_docs");
        for (DocumentKey key : unknownDocs) {
            key.toXContent(builder, params);
        }
        builder.endArray();
        builder.startArray("hits");
        for (RatedSearchHit hit : hits) {
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
        return Objects.equals(id, other.id) &&
                Objects.equals(qualityLevel, other.qualityLevel) &&
                Objects.equals(unknownDocs, other.unknownDocs) &&
                Objects.equals(hits, other.hits) &&
                Objects.equals(optionalMetricDetails, other.optionalMetricDetails);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(id, qualityLevel, unknownDocs, hits, optionalMetricDetails);
    }
}
