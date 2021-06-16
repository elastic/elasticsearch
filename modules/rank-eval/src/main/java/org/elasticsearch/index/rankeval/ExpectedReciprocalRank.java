/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.index.rankeval.EvaluationMetric.joinHitsWithRatings;

/**
 * Implementation of the Expected Reciprocal Rank metric described in:<p>
 *
 * Chapelle, O., Metlzer, D., Zhang, Y., &amp; Grinspan, P. (2009).<br>
 * Expected reciprocal rank for graded relevance.<br>
 * Proceeding of the 18th ACM Conference on Information and Knowledge Management - CIKM ’09, 621.<br>
 * https://doi.org/10.1145/1645953.1646033
 */
public class ExpectedReciprocalRank implements EvaluationMetric {

    /** the default search window size */
    private static final int DEFAULT_K = 10;

    /** the search window size */
    private final int k;

    /**
     * Optional. If set, this will be the rating for docs that are unrated in the ranking evaluation request
     */
    private final Integer unknownDocRating;

    private final int maxRelevance;

    private final double two_pow_maxRelevance;

    public static final String NAME = "expected_reciprocal_rank";

    /**
     * @param maxRelevance the highest expected relevance in the data
     */
    public ExpectedReciprocalRank(int maxRelevance) {
        this(maxRelevance, null, DEFAULT_K);
    }

    /**
     * @param maxRelevance
     *            the maximal relevance judgment in the evaluation dataset
     * @param unknownDocRating
     *            the rating for documents the user hasn't supplied an explicit
     *            rating for. Can be {@code null}, in which case document is
     *            skipped.
     * @param k
     *            the search window size all request use.
     */
    public ExpectedReciprocalRank(int maxRelevance, @Nullable Integer unknownDocRating, int k) {
        this.maxRelevance = maxRelevance;
        this.unknownDocRating = unknownDocRating;
        this.k = k;
        // we can pre-calculate the constant used in metric calculation
        this.two_pow_maxRelevance = Math.pow(2, this.maxRelevance);
    }

    ExpectedReciprocalRank(StreamInput in) throws IOException {
        this.maxRelevance = in.readVInt();
        this.unknownDocRating = in.readOptionalVInt();
        this.k = in.readVInt();
        this.two_pow_maxRelevance = Math.pow(2, this.maxRelevance);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(maxRelevance);
        out.writeOptionalVInt(unknownDocRating);
        out.writeVInt(k);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    int getK() {
        return this.k;
    }

    int getMaxRelevance() {
        return this.maxRelevance;
    }

    /**
     * get the rating used for unrated documents
     */
    public Integer getUnknownDocRating() {
        return this.unknownDocRating;
    }


    @Override
    public OptionalInt forcedSearchSize() {
        return OptionalInt.of(k);
    }

    @Override
    public EvalQueryQuality evaluate(String taskId, SearchHit[] hits, List<RatedDocument> ratedDocs) {
        List<RatedSearchHit> ratedHits = joinHitsWithRatings(hits, ratedDocs);
        if (ratedHits.size() > this.k) {
            ratedHits = ratedHits.subList(0, k);
        }
        List<Integer> ratingsInSearchHits = new ArrayList<>(ratedHits.size());
        int unratedResults = 0;
        for (RatedSearchHit hit : ratedHits) {
            if (hit.getRating().isPresent()) {
                ratingsInSearchHits.add(hit.getRating().getAsInt());
            } else {
                // unknownDocRating might be null, in which case unrated docs will be ignored in the dcg calculation.
                // we still need to add them as a placeholder so the rank of the subsequent ratings is correct
                ratingsInSearchHits.add(unknownDocRating);
            }
            if (hit.getRating().isPresent() == false) {
                unratedResults++;
            }
        }

        double p = 1;
        double err = 0;
        int rank = 1;
        for (Integer rating : ratingsInSearchHits) {
            if (rating != null) {
                double probR = probabilityOfRelevance(rating);
                err = err + (p * probR / rank);
                p = p * (1 - probR);
            }
            rank++;
        }

        EvalQueryQuality evalQueryQuality = new EvalQueryQuality(taskId, err);
        evalQueryQuality.addHitsAndRatings(ratedHits);
        evalQueryQuality.setMetricDetails(new Detail(unratedResults));
        return evalQueryQuality;
    }

    double probabilityOfRelevance(Integer rating) {
        return (Math.pow(2, rating) - 1) / this.two_pow_maxRelevance;
    }

    private static final ParseField K_FIELD = new ParseField("k");
    private static final ParseField UNKNOWN_DOC_RATING_FIELD = new ParseField("unknown_doc_rating");
    private static final ParseField MAX_RELEVANCE_FIELD = new ParseField("maximum_relevance");
    private static final ConstructingObjectParser<ExpectedReciprocalRank, Void> PARSER = new ConstructingObjectParser<>("dcg", false,
            args -> {
                int maxRelevance = (Integer) args[0];
                Integer optK = (Integer) args[2];
                return new ExpectedReciprocalRank(maxRelevance, (Integer) args[1],
                        optK == null ? DEFAULT_K : optK);
            });


    static {
        PARSER.declareInt(constructorArg(), MAX_RELEVANCE_FIELD);
        PARSER.declareInt(optionalConstructorArg(), UNKNOWN_DOC_RATING_FIELD);
        PARSER.declareInt(optionalConstructorArg(), K_FIELD);
    }

    public static ExpectedReciprocalRank fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(NAME);
        builder.field(MAX_RELEVANCE_FIELD.getPreferredName(), this.maxRelevance);
        if (unknownDocRating != null) {
            builder.field(UNKNOWN_DOC_RATING_FIELD.getPreferredName(), this.unknownDocRating);
        }
        builder.field(K_FIELD.getPreferredName(), this.k);
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
        ExpectedReciprocalRank other = (ExpectedReciprocalRank) obj;
        return this.k == other.k &&
                this.maxRelevance == other.maxRelevance
                && Objects.equals(unknownDocRating, other.unknownDocRating);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(unknownDocRating, k, maxRelevance);
    }

    public static final class Detail implements MetricDetail {

        private static ParseField UNRATED_FIELD = new ParseField("unrated_docs");
        private final int unratedDocs;

        Detail(int unratedDocs) {
            this.unratedDocs = unratedDocs;
        }

        Detail(StreamInput in) throws IOException {
            this.unratedDocs = in.readVInt();
        }

        @Override
        public
        String getMetricName() {
            return NAME;
        }

        @Override
        public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field(UNRATED_FIELD.getPreferredName(), this.unratedDocs);
        }

        private static final ConstructingObjectParser<Detail, Void> PARSER = new ConstructingObjectParser<>(NAME, true, args -> {
            return new Detail((Integer) args[0]);
        });

        static {
            PARSER.declareInt(constructorArg(), UNRATED_FIELD);
        }

        public static Detail fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(this.unratedDocs);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        /**
         * @return the number of unrated documents in the search results
         */
        public Object getUnratedDocs() {
            return this.unratedDocs;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            ExpectedReciprocalRank.Detail other = (ExpectedReciprocalRank.Detail) obj;
            return this.unratedDocs == other.unratedDocs;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.unratedDocs);
        }
    }
}

