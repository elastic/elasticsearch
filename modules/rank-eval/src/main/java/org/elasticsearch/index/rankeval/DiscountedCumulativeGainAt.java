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
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DiscountedCumulativeGainAt extends RankedListQualityMetric {

    /** rank position up to which to check results. */
    private int position;
    /** If set to true, the dcg will be normalized (ndcg) */
    private boolean normalize;
    /** If set to, this will be the rating for docs the user hasn't supplied an explicit rating for */
    private Integer unknownDocRating;

    public static final String NAME = "dcg_at_n";
    private static final double LOG2 = Math.log(2.0);

    public DiscountedCumulativeGainAt(StreamInput in) throws IOException {
        position = in.readInt();
        normalize = in.readBoolean();
        unknownDocRating = in.readOptionalVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(position);
        out.writeBoolean(normalize);
        out.writeOptionalVInt(unknownDocRating);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * Initialises position with 10
     * */
    public DiscountedCumulativeGainAt() {
        this.position = 10;
    }

    /**
     * @param position number of top results to check against a given set of relevant results. Must be positive.
     */
    public DiscountedCumulativeGainAt(int position) {
        if (position <= 0) {
            throw new IllegalArgumentException("number of results to check needs to be positive but was " + position);
        }
        this.position = position;
    }

    /**
     * Return number of search results to check for quality metric.
     */
    public int getPosition() {
        return this.position;
    }

    /**
     * set number of search results to check for quality metric.
     */
    public void setPosition(int position) {
        this.position = position;
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
     * check whether this metric computes only dcg or "normalized" ndcg
     */
    public Integer getUnknownDocRating() {
        return this.unknownDocRating;
    }

    @Override
    public EvalQueryQuality evaluate(SearchHit[] hits, List<RatedDocument> ratedDocs) {
        Map<String, RatedDocument> ratedDocsById = new HashMap<>();
        for (RatedDocument doc : ratedDocs) {
            ratedDocsById.put(doc.getDocID(), doc);
        }

        Collection<String> unknownDocIds = new ArrayList<>();
        List<Integer> ratings = new ArrayList<>();
        for (int i = 0; (i < position && i < hits.length); i++) {
            String id = hits[i].getId();
            RatedDocument ratedDoc = ratedDocsById.get(id);
            if (ratedDoc != null) {
                ratings.add(ratedDoc.getRating());
            } else {
                unknownDocIds.add(id);
                if (unknownDocRating != null) {
                    ratings.add(unknownDocRating);
                }
            }
        }
        double dcg = computeDCG(ratings);

        if (normalize) {
            Collections.sort(ratings, Collections.reverseOrder());
            double idcg = computeDCG(ratings);
            dcg = dcg / idcg;
        }
        return new EvalQueryQuality(dcg, unknownDocIds);
    }

    private static double computeDCG(List<Integer> ratings) {
        int rank = 1;
        double dcg = 0;
        for (int rating : ratings) {
            dcg += (Math.pow(2, rating) - 1) / ((Math.log(rank + 1) / LOG2));
            rank++;
        }
        return dcg;
    }

    private static final ParseField SIZE_FIELD = new ParseField("size");
    private static final ParseField NORMALIZE_FIELD = new ParseField("normalize");
    private static final ParseField UNKNOWN_DOC_RATING_FIELD = new ParseField("unknown_doc_rating");
    private static final ObjectParser<DiscountedCumulativeGainAt, ParseFieldMatcherSupplier> PARSER =
            new ObjectParser<>("dcg_at", () -> new DiscountedCumulativeGainAt());

    static {
        PARSER.declareInt(DiscountedCumulativeGainAt::setPosition, SIZE_FIELD);
        PARSER.declareBoolean(DiscountedCumulativeGainAt::setNormalize, NORMALIZE_FIELD);
        PARSER.declareInt(DiscountedCumulativeGainAt::setUnknownDocRating, UNKNOWN_DOC_RATING_FIELD);
    }

    public static DiscountedCumulativeGainAt fromXContent(XContentParser parser, ParseFieldMatcherSupplier matcher) {
        return PARSER.apply(parser, matcher);
    }
}
