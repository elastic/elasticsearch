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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.index.rankeval.RankedListQualityMetric.joinHitsWithRatings;

public class DiscountedCumulativeGain implements RankedListQualityMetric {

    /** If set to true, the dcg will be normalized (ndcg) */
    private boolean normalize;
    /**
     * If set to, this will be the rating for docs the user hasn't supplied an
     * explicit rating for
     */
    private Integer unknownDocRating;

    public static final String NAME = "dcg";
    private static final double LOG2 = Math.log(2.0);

    public DiscountedCumulativeGain() {
    }

    /**
     * @param normalize
     *            If set to true, dcg will be normalized (ndcg) See
     *            https://en.wikipedia.org/wiki/Discounted_cumulative_gain
     * @param unknownDocRating
     *            the rating for docs the user hasn't supplied an explicit
     *            rating for
     */
    public DiscountedCumulativeGain(boolean normalize, Integer unknownDocRating) {
        this.normalize = normalize;
        this.unknownDocRating = unknownDocRating;
    }

    public DiscountedCumulativeGain(StreamInput in) throws IOException {
        normalize = in.readBoolean();
        unknownDocRating = in.readOptionalVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(normalize);
        out.writeOptionalVInt(unknownDocRating);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * If set to true, the dcg will be normalized (ndcg)
     */
    public void setNormalize(boolean normalize) {
        this.normalize = normalize;
    }

    /**
     * check whether this metric computes only dcg or "normalized" ndcg
     */
    public boolean getNormalize() {
        return this.normalize;
    }

    /**
     * the rating for docs the user hasn't supplied an explicit rating for
     */
    public void setUnknownDocRating(int unknownDocRating) {
        this.unknownDocRating = unknownDocRating;
    }

    /**
     * get the rating used for unrated documents
     */
    public Integer getUnknownDocRating() {
        return this.unknownDocRating;
    }

    @Override
    public EvalQueryQuality evaluate(String taskId, SearchHit[] hits,
            List<RatedDocument> ratedDocs) {
        List<Integer> allRatings = ratedDocs.stream().mapToInt(RatedDocument::getRating).boxed()
                .collect(Collectors.toList());
        List<RatedSearchHit> ratedHits = joinHitsWithRatings(hits, ratedDocs);
        List<Integer> ratingsInSearchHits = new ArrayList<>(ratedHits.size());
        for (RatedSearchHit hit : ratedHits) {
            // unknownDocRating might be null, which means it will be unrated
            // docs are ignored in the dcg calculation
            // we still need to add them as a placeholder so the rank of the
            // subsequent ratings is correct
            ratingsInSearchHits.add(hit.getRating().orElse(unknownDocRating));
        }
        double dcg = computeDCG(ratingsInSearchHits);

        if (normalize) {
            Collections.sort(allRatings, Comparator.nullsLast(Collections.reverseOrder()));
            double idcg = computeDCG(
                    allRatings.subList(0, Math.min(ratingsInSearchHits.size(), allRatings.size())));
            dcg = dcg / idcg;
        }
        EvalQueryQuality evalQueryQuality = new EvalQueryQuality(taskId, dcg);
        evalQueryQuality.addHitsAndRatings(ratedHits);
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

    private static final ParseField NORMALIZE_FIELD = new ParseField("normalize");
    private static final ParseField UNKNOWN_DOC_RATING_FIELD = new ParseField("unknown_doc_rating");
    private static final ObjectParser<DiscountedCumulativeGain, Void> PARSER = new ObjectParser<>(
            "dcg_at", () -> new DiscountedCumulativeGain());

    static {
        PARSER.declareBoolean(DiscountedCumulativeGain::setNormalize, NORMALIZE_FIELD);
        PARSER.declareInt(DiscountedCumulativeGain::setUnknownDocRating, UNKNOWN_DOC_RATING_FIELD);
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
                && Objects.equals(unknownDocRating, other.unknownDocRating);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(normalize, unknownDocRating);
    }

    // TODO maybe also add debugging breakdown here
}
