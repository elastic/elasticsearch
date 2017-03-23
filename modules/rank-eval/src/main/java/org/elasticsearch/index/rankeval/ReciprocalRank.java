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
 * Evaluate reciprocal rank. By default documents with a rating equal or bigger
 * than 1 are considered to be "relevant" for the reciprocal rank calculation.
 * This value can be changes using the "relevant_rating_threshold" parameter.
 */
public class ReciprocalRank implements RankedListQualityMetric {

    public static final String NAME = "reciprocal_rank";

    /** ratings equal or above this value will be considered relevant. */
    private int relevantRatingThreshhold = 1;

    /**
     * Initializes maxAcceptableRank with 10
     */
    public ReciprocalRank() {
        // use defaults
    }

    public ReciprocalRank(StreamInput in) throws IOException {
        this.relevantRatingThreshhold = in.readVInt();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * Sets the rating threshold above which ratings are considered to be
     * "relevant" for this metric.
     */
    public void setRelevantRatingThreshhold(int threshold) {
        if (threshold < 0) {
            throw new IllegalArgumentException(
                    "Relevant rating threshold for precision must be positive integer.");
        }

        this.relevantRatingThreshhold = threshold;
    }

    /**
     * Return the rating threshold above which ratings are considered to be
     * "relevant" for this metric. Defaults to 1.
     */
    public int getRelevantRatingThreshold() {
        return relevantRatingThreshhold;
    }

    /**
     * Compute ReciprocalRank based on provided relevant document IDs.
     *
     * @return reciprocal Rank for above {@link SearchResult} list.
     **/
    @Override
    public EvalQueryQuality evaluate(String taskId, SearchHit[] hits,
            List<RatedDocument> ratedDocs) {
        List<RatedSearchHit> ratedHits = joinHitsWithRatings(hits, ratedDocs);
        int firstRelevant = -1;
        int rank = 1;
        for (RatedSearchHit hit : ratedHits) {
            Optional<Integer> rating = hit.getRating();
            if (rating.isPresent()) {
                if (rating.get() >= this.relevantRatingThreshhold) {
                    firstRelevant = rank;
                    break;
                }
            }
            rank++;
        }

        double reciprocalRank = (firstRelevant == -1) ? 0 : 1.0d / firstRelevant;
        EvalQueryQuality evalQueryQuality = new EvalQueryQuality(taskId, reciprocalRank);
        evalQueryQuality.addMetricDetails(new Breakdown(firstRelevant));
        evalQueryQuality.addHitsAndRatings(ratedHits);
        return evalQueryQuality;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(relevantRatingThreshhold);
    }

    private static final ParseField RELEVANT_RATING_FIELD = new ParseField(
            "relevant_rating_threshold");
    private static final ObjectParser<ReciprocalRank, Void> PARSER = new ObjectParser<>(
            "reciprocal_rank", () -> new ReciprocalRank());

    static {
        PARSER.declareInt(ReciprocalRank::setRelevantRatingThreshhold, RELEVANT_RATING_FIELD);
    }

    public static ReciprocalRank fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(NAME);
        builder.field(RELEVANT_RATING_FIELD.getPreferredName(), this.relevantRatingThreshhold);
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
        ReciprocalRank other = (ReciprocalRank) obj;
        return Objects.equals(relevantRatingThreshhold, other.relevantRatingThreshhold);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(relevantRatingThreshhold);
    }

    public static class Breakdown implements MetricDetails {

        private int firstRelevantRank;

        public Breakdown(int firstRelevantRank) {
            this.firstRelevantRank = firstRelevantRank;
        }

        public Breakdown(StreamInput in) throws IOException {
            this.firstRelevantRank = in.readVInt();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params)
                throws IOException {
            builder.field("first_relevant", firstRelevantRank);
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(firstRelevantRank);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        public int getFirstRelevantRank() {
            return firstRelevantRank;
        }

        @Override
        public final boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            ReciprocalRank.Breakdown other = (ReciprocalRank.Breakdown) obj;
            return Objects.equals(firstRelevantRank, other.firstRelevantRank);
        }

        @Override
        public final int hashCode() {
            return Objects.hash(firstRelevantRank);
        }
    }
}
