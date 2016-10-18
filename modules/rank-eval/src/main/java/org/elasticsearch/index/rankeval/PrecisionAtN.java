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
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
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
 * Evaluate Precision at N, N being the number of search results to consider for precision calculation.
 * Documents of unkonwn quality are ignored in the precision at n computation and returned by document id.
 * By default documents with a rating equal or bigger than 1 are considered to be "relevant" for the precision
 * calculation. This value can be changes using the "relevant_rating_threshold" parameter.
 * */
public class PrecisionAtN implements RankedListQualityMetric {

    /** Number of results to check against a given set of relevant results. */
    private int n;

    /** ratings equal or above this value will be considered relevant. */
    private int relevantRatingThreshhold = 1;

    public static final String NAME = "precision_atn";

    private static final ParseField SIZE_FIELD = new ParseField("size");
    private static final ParseField RELEVANT_RATING_FIELD = new ParseField("relevant_rating_threshold");
    private static final ConstructingObjectParser<PrecisionAtN, ParseFieldMatcherSupplier> PARSER = new ConstructingObjectParser<>(
            "precision_at", a -> new PrecisionAtN((Integer) a[0]));

    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), SIZE_FIELD);
        PARSER.declareInt(PrecisionAtN::setRelevantRatingThreshhold, RELEVANT_RATING_FIELD);
    }

    public PrecisionAtN(StreamInput in) throws IOException {
        n = in.readInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(n);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * Initialises n with 10
     * */
    public PrecisionAtN() {
        this.n = 10;
    }

    /**
     * @param n number of top results to check against a given set of relevant results.
     * */
    public PrecisionAtN(int n) {
        this.n= n;
    }

    /**
     * Return number of search results to check for quality.
     * */
    public int getN() {
        return n;
    }

    /**
     * Sets the rating threshold above which ratings are considered to be "relevant" for this metric.
     * */
    public void setRelevantRatingThreshhold(int threshold) {
        this.relevantRatingThreshhold = threshold;
    }

    /**
     * Return the rating threshold above which ratings are considered to be "relevant" for this metric.
     * Defaults to 1.
     * */
    public int getRelevantRatingThreshold() {
        return relevantRatingThreshhold ;
    }

    public static PrecisionAtN fromXContent(XContentParser parser, ParseFieldMatcherSupplier matcher) {
        return PARSER.apply(parser, matcher);
    }

    /** Compute precisionAtN based on provided relevant document IDs.
     * @return precision at n for above {@link SearchResult} list.
     **/
    @Override
    public EvalQueryQuality evaluate(String taskId, SearchHit[] hits, List<RatedDocument> ratedDocs) {
        int good = 0;
        int bad = 0;
        List<RatedSearchHit> ratedSearchHits = joinHitsWithRatings(hits, ratedDocs);
        for (RatedSearchHit hit : ratedSearchHits) {
            Optional<Integer> rating = hit.getRating();
            if (rating.isPresent()) {
                if (rating.get() >= this.relevantRatingThreshhold) {
                    good++;
                } else {
                    bad++;
                }
            }
        }
        double precision = (double) good / (good + bad);
        EvalQueryQuality evalQueryQuality = new EvalQueryQuality(taskId, precision);
        evalQueryQuality.addMetricDetails(new PrecisionAtN.Breakdown(good, good + bad));
        evalQueryQuality.addHitsAndRatings(ratedSearchHits);
        return evalQueryQuality;
    }

    // TODO add abstraction that also works for other metrics
    public enum Rating {
        IRRELEVANT, RELEVANT;
    }

    /**
     * Needed to get the enum accross serialisation boundaries.
     * */
    public static class RatingMapping {
        public static Integer mapFrom(Rating rating) {
            if (Rating.RELEVANT.equals(rating)) {
                return 1;
            }
            return 0;
        }

        public static Rating mapTo(Integer rating) {
            if (rating == 1) {
                return Rating.RELEVANT;
            }
            return Rating.IRRELEVANT;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(NAME);
        builder.field(SIZE_FIELD.getPreferredName(), this.n);
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
        PrecisionAtN other = (PrecisionAtN) obj;
        return Objects.equals(n, other.n);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(n);
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
            PrecisionAtN.Breakdown other = (PrecisionAtN.Breakdown) obj;
            return Objects.equals(relevantRetrieved, other.relevantRetrieved) &&
                    Objects.equals(retrieved, other.retrieved);
        }

        @Override
        public final int hashCode() {
            return Objects.hash(relevantRetrieved, retrieved);
        }
    }
}
