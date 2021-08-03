/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchHit;

import javax.naming.directory.SearchResult;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.index.rankeval.EvaluationMetric.joinHitsWithRatings;

/**
 * Metric implementing Precision@K
 * (https://en.wikipedia.org/wiki/Evaluation_measures_(information_retrieval)#Precision).<br>
 * By default documents with a rating equal or bigger than 1 are considered to
 * be "relevant" for this calculation. This value can be changed using the
 * `relevant_rating_threshold` parameter.<br>
 * The `ignore_unlabeled` parameter (default to false) controls if unrated
 * documents should be ignored.
 * The `k` parameter (defaults to 10) controls the search window size.
 */
public class PrecisionAtK implements EvaluationMetric {

    public static final String NAME = "precision";

    private static final int DEFAULT_RELEVANT_RATING_THRESHOLD = 1;
    private static final boolean DEFAULT_IGNORE_UNLABELED = false;
    private static final int DEFAULT_K = 10;

    private static final ParseField RELEVANT_RATING_THRESHOLD_FIELD = new ParseField("relevant_rating_threshold");
    private static final ParseField IGNORE_UNLABELED_FIELD = new ParseField("ignore_unlabeled");
    private static final ParseField K_FIELD = new ParseField("k");

    private final int relevantRatingThreshold;
    private final boolean ignoreUnlabeled;
    private final int k;

    /**
     * Metric implementing Precision@K.
     * @param relevantRatingThreshold
     *            ratings equal or above this value will be considered relevant.
     * @param ignoreUnlabeled
     *            Controls how unlabeled documents in the search hits are treated.
     *            Set to 'true', unlabeled documents are ignored and neither count
     *            as true or false positives. Set to 'false', they are treated as
     *            false positives.
     * @param k
     *            controls the window size for the search results the metric takes into account
     */
    public PrecisionAtK(int relevantRatingThreshold, boolean ignoreUnlabeled, int k) {
        if (relevantRatingThreshold < 0) {
            throw new IllegalArgumentException("Relevant rating threshold for precision must be positive integer.");
        }
        if (k <= 0) {
            throw new IllegalArgumentException("Window size k must be positive.");
        }
        this.relevantRatingThreshold = relevantRatingThreshold;
        this.ignoreUnlabeled = ignoreUnlabeled;
        this.k = k;
    }

    public PrecisionAtK(boolean ignoreUnlabeled) {
        this(DEFAULT_RELEVANT_RATING_THRESHOLD, ignoreUnlabeled, DEFAULT_K);
    }

    public PrecisionAtK() {
        this(DEFAULT_RELEVANT_RATING_THRESHOLD, DEFAULT_IGNORE_UNLABELED, DEFAULT_K);
    }

    PrecisionAtK(StreamInput in) throws IOException {
        this(in.readVInt(), in.readBoolean(), in.readVInt());
    }

    private static final ConstructingObjectParser<PrecisionAtK, Void> PARSER = new ConstructingObjectParser<>(NAME, args -> {
        Integer relevantRatingThreshold = (Integer) args[0];
        Boolean ignoreUnlabeled = (Boolean) args[1];
        Integer k = (Integer) args[2];
        return new PrecisionAtK(
            relevantRatingThreshold == null ? DEFAULT_RELEVANT_RATING_THRESHOLD : relevantRatingThreshold,
            ignoreUnlabeled == null ? DEFAULT_IGNORE_UNLABELED : ignoreUnlabeled,
            k == null ? DEFAULT_K : k);
    });

    static {
        PARSER.declareInt(optionalConstructorArg(), RELEVANT_RATING_THRESHOLD_FIELD);
        PARSER.declareBoolean(optionalConstructorArg(), IGNORE_UNLABELED_FIELD);
        PARSER.declareInt(optionalConstructorArg(), K_FIELD);
    }

    public static PrecisionAtK fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(getRelevantRatingThreshold());
        out.writeBoolean(getIgnoreUnlabeled());
        out.writeVInt(getK());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(NAME);
        builder.field(RELEVANT_RATING_THRESHOLD_FIELD.getPreferredName(), getRelevantRatingThreshold());
        builder.field(IGNORE_UNLABELED_FIELD.getPreferredName(), getIgnoreUnlabeled());
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

    /**
     * Gets the 'ignore_unlabeled' parameter.
     */
    public boolean getIgnoreUnlabeled() {
        return ignoreUnlabeled;
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
     * Should we count unlabeled documents? This is the inverse of {@link #getIgnoreUnlabeled()}.
     */
    private boolean shouldCountUnlabeled() {
        return getIgnoreUnlabeled() == false;
    }

    /**
     * Compute precision at k based on provided relevant document IDs.
     *
     * @return precision at k for above {@link SearchResult} list.
     **/
    @Override
    public EvalQueryQuality evaluate(String taskId, SearchHit[] hits,
                                     List<RatedDocument> ratedDocs) {

        List<RatedSearchHit> ratedSearchHits = joinHitsWithRatings(hits, ratedDocs);

        int relevantRetrieved = 0;
        int retrieved = 0;

        for (RatedSearchHit hit : ratedSearchHits) {
            OptionalInt rating = hit.getRating();
            if (rating.isPresent()) {
                retrieved++;
                if (isRelevant(rating.getAsInt())) {
                    relevantRetrieved++;
                }
            } else if (shouldCountUnlabeled()) {
                retrieved++;
            }
        }

        double precision = 0.0;
        if (retrieved > 0) {
            precision = (double) relevantRetrieved / retrieved;
        }

        EvalQueryQuality evalQueryQuality = new EvalQueryQuality(taskId, precision);
        evalQueryQuality.setMetricDetails(new PrecisionAtK.Detail(relevantRetrieved, retrieved));
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
        PrecisionAtK other = (PrecisionAtK) obj;
        return Objects.equals(relevantRatingThreshold, other.relevantRatingThreshold)
            && Objects.equals(ignoreUnlabeled, other.ignoreUnlabeled)
            && Objects.equals(k, other.k);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(relevantRatingThreshold, ignoreUnlabeled, k);
    }

    public static final class Detail implements MetricDetail {

        private static final ParseField RELEVANT_DOCS_RETRIEVED_FIELD = new ParseField("relevant_docs_retrieved");
        private static final ParseField DOCS_RETRIEVED_FIELD = new ParseField("docs_retrieved");

        private int relevantRetrieved;
        private int retrieved;

        Detail(int relevantRetrieved, int retrieved) {
            this.relevantRetrieved = relevantRetrieved;
            this.retrieved = retrieved;
        }

        Detail(StreamInput in) throws IOException {
            this(in.readVInt(), in.readVInt());
        }

        private static final ConstructingObjectParser<Detail, Void> PARSER =
            new ConstructingObjectParser<>(NAME, true, args -> new Detail((Integer) args[0], (Integer) args[1]));

        static {
            PARSER.declareInt(constructorArg(), RELEVANT_DOCS_RETRIEVED_FIELD);
            PARSER.declareInt(constructorArg(), DOCS_RETRIEVED_FIELD);
        }

        public static Detail fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(relevantRetrieved);
            out.writeVLong(retrieved);
        }

        @Override
        public XContentBuilder innerToXContent(XContentBuilder builder, Params params)
            throws IOException {
            builder.field(RELEVANT_DOCS_RETRIEVED_FIELD.getPreferredName(), relevantRetrieved);
            builder.field(DOCS_RETRIEVED_FIELD.getPreferredName(), retrieved);
            return builder;
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
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            PrecisionAtK.Detail other = (PrecisionAtK.Detail) obj;
            return Objects.equals(relevantRetrieved, other.relevantRetrieved)
                    && Objects.equals(retrieved, other.retrieved);
        }

        @Override
        public int hashCode() {
            return Objects.hash(relevantRetrieved, retrieved);
        }
    }
}
