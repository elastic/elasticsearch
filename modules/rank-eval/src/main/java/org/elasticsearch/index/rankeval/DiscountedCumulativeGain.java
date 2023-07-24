/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import static org.elasticsearch.index.rankeval.EvaluationMetric.joinHitsWithRatings;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Metric implementing Discounted Cumulative Gain.
 * The `normalize` parameter can be set to calculate the normalized NDCG (set to {@code false} by default).<br>
 * The optional `unknown_doc_rating` parameter can be used to specify a default rating for unlabeled documents.
 * @see <a href="https://en.wikipedia.org/wiki/Discounted_cumulative_gain#Discounted_Cumulative_Gain">Discounted Cumulative Gain</a><br>
 */
public class DiscountedCumulativeGain implements EvaluationMetric {

    /** If set to true, the dcg will be normalized (ndcg) */
    private final boolean normalize;

    /** the default search window size */
    private static final int DEFAULT_K = 10;

    /** the search window size */
    private final int k;

    /**
     * Optional. If set, this will be the rating for docs that are unrated in the ranking evaluation request
     */
    private final Integer unknownDocRating;

    public static final String NAME = "dcg";
    private static final double LOG2 = Math.log(2.0);

    public DiscountedCumulativeGain() {
        this(false, null, DEFAULT_K);
    }

    /**
     * @param normalize
     *            If set to true, dcg will be normalized (ndcg) See
     *            https://en.wikipedia.org/wiki/Discounted_cumulative_gain
     * @param unknownDocRating
     *            the rating for documents the user hasn't supplied an explicit
     *            rating for
     * @param k the search window size all request use.
     */
    public DiscountedCumulativeGain(boolean normalize, Integer unknownDocRating, int k) {
        this.normalize = normalize;
        this.unknownDocRating = unknownDocRating;
        this.k = k;
    }

    DiscountedCumulativeGain(StreamInput in) throws IOException {
        normalize = in.readBoolean();
        unknownDocRating = in.readOptionalVInt();
        k = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(normalize);
        out.writeOptionalVInt(unknownDocRating);
        out.writeVInt(k);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    boolean getNormalize() {
        return this.normalize;
    }

    int getK() {
        return this.k;
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
        final double dcg = computeDCG(ratingsInSearchHits);
        double result = dcg;
        double idcg = 0;

        if (normalize) {
            List<Integer> allRatings = ratedDocs.stream()
                .map(RatedDocument::getRating)
                .sorted(Collections.reverseOrder())
                .collect(Collectors.toList());
            idcg = computeDCG(allRatings.subList(0, Math.min(ratingsInSearchHits.size(), allRatings.size())));
            if (idcg != 0) {
                result = dcg / idcg;
            } else {
                result = 0;
            }
        }
        EvalQueryQuality evalQueryQuality = new EvalQueryQuality(taskId, result);
        evalQueryQuality.addHitsAndRatings(ratedHits);
        evalQueryQuality.setMetricDetails(new Detail(dcg, idcg, unratedResults));
        return evalQueryQuality;
    }

    private static double computeDCG(List<Integer> ratings) {
        int rank = 1;
        double dcg = 0;
        for (Integer rating : ratings) {
            if (rating != null) {
                dcg += (Math.pow(2, rating) - 1) / ((Math.log(rank + 1) / LOG2));
            }
            rank++;
        }
        return dcg;
    }

    private static final ParseField K_FIELD = new ParseField("k");
    private static final ParseField NORMALIZE_FIELD = new ParseField("normalize");
    private static final ParseField UNKNOWN_DOC_RATING_FIELD = new ParseField("unknown_doc_rating");
    private static final ConstructingObjectParser<DiscountedCumulativeGain, Void> PARSER = new ConstructingObjectParser<>(
        "dcg",
        false,
        args -> {
            Boolean normalized = (Boolean) args[0];
            Integer optK = (Integer) args[2];
            return new DiscountedCumulativeGain(
                normalized == null ? false : normalized,
                (Integer) args[1],
                optK == null ? DEFAULT_K : optK
            );
        }
    );

    static {
        PARSER.declareBoolean(optionalConstructorArg(), NORMALIZE_FIELD);
        PARSER.declareInt(optionalConstructorArg(), UNKNOWN_DOC_RATING_FIELD);
        PARSER.declareInt(optionalConstructorArg(), K_FIELD);
    }

    public static DiscountedCumulativeGain fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(NAME);
        builder.field(NORMALIZE_FIELD.getPreferredName(), this.normalize);
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
        DiscountedCumulativeGain other = (DiscountedCumulativeGain) obj;
        return Objects.equals(normalize, other.normalize)
            && Objects.equals(unknownDocRating, other.unknownDocRating)
            && Objects.equals(k, other.k);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(normalize, unknownDocRating, k);
    }

    public static final class Detail implements MetricDetail {

        private static ParseField DCG_FIELD = new ParseField("dcg");
        private static ParseField IDCG_FIELD = new ParseField("ideal_dcg");
        private static ParseField NDCG_FIELD = new ParseField("normalized_dcg");
        private static ParseField UNRATED_FIELD = new ParseField("unrated_docs");
        private final double dcg;
        private final double idcg;
        private final int unratedDocs;

        Detail(double dcg, double idcg, int unratedDocs) {
            this.dcg = dcg;
            this.idcg = idcg;
            this.unratedDocs = unratedDocs;
        }

        Detail(StreamInput in) throws IOException {
            this.dcg = in.readDouble();
            this.idcg = in.readDouble();
            this.unratedDocs = in.readVInt();
        }

        @Override
        public String getMetricName() {
            return NAME;
        }

        @Override
        public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(DCG_FIELD.getPreferredName(), this.dcg);
            if (this.idcg != 0) {
                builder.field(IDCG_FIELD.getPreferredName(), this.idcg);
                builder.field(NDCG_FIELD.getPreferredName(), this.dcg / this.idcg);
            }
            builder.field(UNRATED_FIELD.getPreferredName(), this.unratedDocs);
            return builder;
        }

        private static final ConstructingObjectParser<Detail, Void> PARSER = new ConstructingObjectParser<>(NAME, true, args -> {
            return new Detail((Double) args[0], (Double) args[1] != null ? (Double) args[1] : 0.0d, (Integer) args[2]);
        });

        static {
            PARSER.declareDouble(constructorArg(), DCG_FIELD);
            PARSER.declareDouble(optionalConstructorArg(), IDCG_FIELD);
            PARSER.declareInt(constructorArg(), UNRATED_FIELD);
        }

        public static Detail fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(this.dcg);
            out.writeDouble(this.idcg);
            out.writeVInt(this.unratedDocs);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        /**
         * @return the discounted cumulative gain
         */
        public double getDCG() {
            return this.dcg;
        }

        /**
         * @return the ideal discounted cumulative gain, can be 0 if nothing was computed, e.g. because no normalization was required
         */
        public double getIDCG() {
            return this.idcg;
        }

        /**
         * @return the normalized discounted cumulative gain, can be 0 if nothing was computed, e.g. because no normalization was required
         */
        public double getNDCG() {
            return (this.idcg != 0) ? this.dcg / this.idcg : 0;
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
            DiscountedCumulativeGain.Detail other = (DiscountedCumulativeGain.Detail) obj;
            return Double.compare(this.dcg, other.dcg) == 0
                && Double.compare(this.idcg, other.idcg) == 0
                && this.unratedDocs == other.unratedDocs;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.dcg, this.idcg, this.unratedDocs);
        }
    }
}
