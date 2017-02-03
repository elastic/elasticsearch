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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.naming.directory.SearchResult;

import static org.elasticsearch.index.rankeval.RankedListQualityMetric.joinHitsWithRatings;

/**
 * Evaluate Precision of the search results. Documents without a rating are
 * ignored. By default documents with a rating equal or bigger than 1 are
 * considered to be "relevant" for the precision calculation. This value can be
 * changes using the "relevant_rating_threshold" parameter.
 */
public class Precision implements RankedListQualityMetric {

    public static final String NAME = "precision";

    private static final ParseField RELEVANT_RATING_FIELD = new ParseField("relevant_rating_threshold");
    private static final ParseField IGNORE_UNLABELED_FIELD = new ParseField("ignore_unlabeled");
    private static final ObjectParser<Precision, Void> PARSER = new ObjectParser<>(NAME, Precision::new);

    /**
     * This setting controls how unlabeled documents in the search hits are
     * treated. Set to 'true', unlabeled documents are ignored and neither count
     * as true or false positives. Set to 'false', they are treated as false positives.
     */
    private boolean ignoreUnlabeled = false;

    /** ratings equal or above this value will be considered relevant. */
    private int relevantRatingThreshhold = 1;

    public Precision() {
        // needed for supplier in parser
    }

    static {
        PARSER.declareInt(Precision::setRelevantRatingThreshhold, RELEVANT_RATING_FIELD);
        PARSER.declareBoolean(Precision::setIgnoreUnlabeled, IGNORE_UNLABELED_FIELD);
    }

    public Precision(StreamInput in) throws IOException {
        relevantRatingThreshhold = in.readOptionalVInt();
        ignoreUnlabeled = in.readOptionalBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalVInt(relevantRatingThreshhold);
        out.writeOptionalBoolean(ignoreUnlabeled);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * Sets the rating threshold above which ratings are considered to be "relevant" for this metric.
     * */
    public void setRelevantRatingThreshhold(int threshold) {
        if (threshold < 0) {
            throw new IllegalArgumentException("Relevant rating threshold for precision must be positive integer.");
        }
        this.relevantRatingThreshhold = threshold;
    }

    /**
     * Return the rating threshold above which ratings are considered to be "relevant" for this metric.
     * Defaults to 1.
     * */
    public int getRelevantRatingThreshold() {
        return relevantRatingThreshhold ;
    }

    /**
     * Sets the 'ìgnore_unlabeled' parameter
     * */
    public void setIgnoreUnlabeled(boolean ignoreUnlabeled) {
        this.ignoreUnlabeled = ignoreUnlabeled;
    }

    /**
     * Gets the 'ìgnore_unlabeled' parameter
     * */
    public boolean getIgnoreUnlabeled() {
        return ignoreUnlabeled;
    }

    public static Precision fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    /** Compute precisionAtN based on provided relevant document IDs.
     * @return precision at n for above {@link SearchResult} list.
     **/
    @Override
    public EvalQueryQuality evaluate(String taskId, SearchHit[] hits, List<RatedDocument> ratedDocs) {
        int truePositives = 0;
        int falsePositives = 0;
        List<RatedSearchHit> ratedSearchHits = joinHitsWithRatings(hits, ratedDocs);
        for (RatedSearchHit hit : ratedSearchHits) {
            Optional<Integer> rating = hit.getRating();
            if (rating.isPresent()) {
                if (rating.get() >= this.relevantRatingThreshhold) {
                    truePositives++;
                } else {
                    falsePositives++;
                }
            } else if (ignoreUnlabeled == false) {
                falsePositives++;
            }
        }
        double precision = 0.0;
        if (truePositives + falsePositives > 0) {
            precision = (double) truePositives / (truePositives + falsePositives);
        }
        EvalQueryQuality evalQueryQuality = new EvalQueryQuality(taskId, precision);
        evalQueryQuality.addMetricDetails(new Precision.Breakdown(truePositives, truePositives + falsePositives));
        evalQueryQuality.addHitsAndRatings(ratedSearchHits);
        return evalQueryQuality;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(NAME);
        builder.field(RELEVANT_RATING_FIELD.getPreferredName(), this.relevantRatingThreshhold);
        builder.field(IGNORE_UNLABELED_FIELD.getPreferredName(), this.ignoreUnlabeled);
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
        Precision other = (Precision) obj;
        return Objects.equals(relevantRatingThreshhold, other.relevantRatingThreshhold) &&
                Objects.equals(ignoreUnlabeled, other.ignoreUnlabeled);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(relevantRatingThreshhold, ignoreUnlabeled);
    }

    public static class Breakdown implements MetricDetails {

        public static final String DOCS_RETRIEVED_FIELD = "docs_retrieved";
        public static final String RELEVANT_DOCS_RETRIEVED_FIELD = "relevant_docs_retrieved";
        private int relevantRetrieved;
        private int retrieved;

        public Breakdown(int relevantRetrieved, int retrieved) {
            this.relevantRetrieved = relevantRetrieved;
            this.retrieved = retrieved;
        }

        public Breakdown(StreamInput in) throws IOException {
            this.relevantRetrieved = in.readVInt();
            this.retrieved = in.readVInt();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(RELEVANT_DOCS_RETRIEVED_FIELD, relevantRetrieved);
            builder.field(DOCS_RETRIEVED_FIELD, retrieved);
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(relevantRetrieved);
            out.writeVInt(retrieved);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        public int getRelevantRetrieved() {
            return relevantRetrieved;
        }

        public int getRetrieved() {
            return retrieved;
        }

        @Override
        public final boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Precision.Breakdown other = (Precision.Breakdown) obj;
            return Objects.equals(relevantRetrieved, other.relevantRetrieved) &&
                    Objects.equals(retrieved, other.retrieved);
        }

        @Override
        public final int hashCode() {
            return Objects.hash(relevantRetrieved, retrieved);
        }
    }
}
