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
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;

import javax.naming.directory.SearchResult;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.index.rankeval.EvaluationMetric.joinHitsWithRatings;

/**
 * Metric implementing Recall@K
 * (https://en.wikipedia.org/wiki/Evaluation_measures_(information_retrieval)#Recall).<br>
 * By default documents with a rating equal or bigger than 1 are considered to
 * be "relevant" for this calculation. This value can be changed using the
 * `relevant_rating_threshold` parameter.<br>
 * The `k` parameter (defaults to 10) controls the search window size.
 */
public class RecallAtK implements EvaluationMetric {

    public static final String NAME = "recall";

    private static final int DEFAULT_RELEVANT_RATING_THRESHOLD = 1;
    private static final int DEFAULT_K = 10;

    private static final ParseField RELEVANT_RATING_THRESHOLD_FIELD = new ParseField("relevant_rating_threshold");
    private static final ParseField K_FIELD = new ParseField("k");

    private final int relevantRatingThreshold;
    private final int k;

    /**
     * Metric implementing Recall@K.
     * @param relevantRatingThreshold
     *            ratings equal or above this value will be considered relevant.
     * @param k
     *            controls the window size for the search results the metric takes into account
     */
    public RecallAtK(int relevantRatingThreshold, int k) {
        if (relevantRatingThreshold < 0) {
            throw new IllegalArgumentException("Relevant rating threshold for precision must be positive integer.");
        }
        if (k <= 0) {
            throw new IllegalArgumentException("Window size k must be positive.");
        }
        this.relevantRatingThreshold = relevantRatingThreshold;
        this.k = k;
    }

    public RecallAtK() {
        this(DEFAULT_RELEVANT_RATING_THRESHOLD, DEFAULT_K);
    }

    RecallAtK(StreamInput in) throws IOException {
        this(in.readVInt(), in.readVInt());
    }

    private static final ConstructingObjectParser<RecallAtK, Void> PARSER = new ConstructingObjectParser<>(NAME, args -> {
        Integer relevantRatingThreshold = (Integer) args[0];
        Integer k = (Integer) args[1];
        return new RecallAtK(
            relevantRatingThreshold == null ? DEFAULT_RELEVANT_RATING_THRESHOLD : relevantRatingThreshold,
            k == null ? DEFAULT_K : k);
    });

    static {
        PARSER.declareInt(optionalConstructorArg(), RELEVANT_RATING_THRESHOLD_FIELD);
        PARSER.declareInt(optionalConstructorArg(), K_FIELD);
    }

    public static RecallAtK fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(getRelevantRatingThreshold());
        out.writeVInt(getK());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(NAME);
        builder.field(RELEVANT_RATING_THRESHOLD_FIELD.getPreferredName(), getRelevantRatingThreshold());
        builder.field(K_FIELD.getPreferredName(), getK());
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * Return the rating threshold above which ratings are considered to be
     * "relevant" for this metric. Defaults to 1.
     */
    public int getRelevantRatingThreshold() {
        return relevantRatingThreshold;
    }

    public int getK() {
        return k;
    }

    @Override
    public OptionalInt forcedSearchSize() {
        return OptionalInt.of(getK());
    }

    /**
     * Binarizes a rating based on the relevant rating threshold.
     */
    private boolean isRelevant(int rating) {
        return rating >= getRelevantRatingThreshold();
    }

    /**
     * Compute recall at k based on provided relevant document IDs.
     *
     * @return recall at k for above {@link SearchResult} list.
     **/
    @Override
    public EvalQueryQuality evaluate(String taskId, SearchHit[] hits,
                                     List<RatedDocument> ratedDocs) {

        List<RatedSearchHit> ratedSearchHits = joinHitsWithRatings(hits, ratedDocs);

        int relevantRetrieved = 0;
        for (RatedSearchHit hit : ratedSearchHits) {
            OptionalInt rating = hit.getRating();
            if (rating.isPresent() && isRelevant(rating.getAsInt())) {
                relevantRetrieved++;
            }
        }

        int relevant = 0;
        for (RatedDocument rd : ratedDocs) {
            if(isRelevant(rd.getRating())) {
                relevant++;
            }
        }

        double recall = 0.0;
        if (relevant > 0) {
            recall = (double) relevantRetrieved / relevant;
        }

        EvalQueryQuality evalQueryQuality = new EvalQueryQuality(taskId, recall);
        evalQueryQuality.setMetricDetails(new RecallAtK.Detail(relevantRetrieved, relevant));
        evalQueryQuality.addHitsAndRatings(ratedSearchHits);
        return evalQueryQuality;
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RecallAtK other = (RecallAtK) obj;
        return Objects.equals(relevantRatingThreshold, other.relevantRatingThreshold)
            && Objects.equals(k, other.k);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(relevantRatingThreshold, k);
    }

    public static final class Detail implements MetricDetail {

        private static final ParseField RELEVANT_DOCS_RETRIEVED_FIELD = new ParseField("relevant_docs_retrieved");
        private static final ParseField RELEVANT_DOCS_FIELD = new ParseField("relevant_docs");
        private long relevantRetrieved;
        private long relevant;

        Detail(long relevantRetrieved, long relevant) {
            this.relevantRetrieved = relevantRetrieved;
            this.relevant = relevant;
        }

        Detail(StreamInput in) throws IOException {
            this.relevantRetrieved = in.readVLong();
            this.relevant = in.readVLong();
        }

        private static final ConstructingObjectParser<Detail, Void> PARSER =
            new ConstructingObjectParser<>(NAME, true, args -> new Detail((Integer) args[0], (Integer) args[1]));

        static {
            PARSER.declareInt(constructorArg(), RELEVANT_DOCS_RETRIEVED_FIELD);
            PARSER.declareInt(constructorArg(), RELEVANT_DOCS_FIELD);
        }

        public static Detail fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(relevantRetrieved);
            out.writeVLong(relevant);
        }

        @Override
        public XContentBuilder innerToXContent(XContentBuilder builder, Params params)
            throws IOException {
            builder.field(RELEVANT_DOCS_RETRIEVED_FIELD.getPreferredName(), relevantRetrieved);
            builder.field(RELEVANT_DOCS_FIELD.getPreferredName(), relevant);
            return builder;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        public long getRelevantRetrieved() {
            return relevantRetrieved;
        }

        public long getRelevant() {
            return relevant;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            RecallAtK.Detail other = (RecallAtK.Detail) obj;
            return Objects.equals(relevantRetrieved, other.relevantRetrieved)
                && Objects.equals(relevant, other.relevant);
        }

        @Override
        public int hashCode() {
            return Objects.hash(relevantRetrieved, relevant);
        }
    }
}
