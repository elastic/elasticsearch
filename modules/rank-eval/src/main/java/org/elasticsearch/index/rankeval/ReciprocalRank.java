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
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.rankeval.PrecisionAtN.Rating;
import org.elasticsearch.index.rankeval.PrecisionAtN.RatingMapping;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.naming.directory.SearchResult;

/**
 * Evaluate reciprocal rank.
 * */
public class ReciprocalRank extends RankedListQualityMetric<ReciprocalRank> {

    public static final String NAME = "reciprocal_rank";
    public static final int DEFAULT_MAX_ACCEPTABLE_RANK = 10;
    private int maxAcceptableRank = DEFAULT_MAX_ACCEPTABLE_RANK;

    /**
     * Initializes maxAcceptableRank with 10
     */
    public ReciprocalRank() {
        // use defaults
    }

    /**
     * @param maxAcceptableRank
     *            maximal acceptable rank. Must be positive.
     */
    public ReciprocalRank(int maxAcceptableRank) {
        if (maxAcceptableRank <= 0) {
            throw new IllegalArgumentException("maximal acceptable rank needs to be positive but was [" + maxAcceptableRank + "]");
        }
        this.maxAcceptableRank = maxAcceptableRank;
    }

    public ReciprocalRank(StreamInput in) throws IOException {
        this.maxAcceptableRank = in.readInt();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * @param maxAcceptableRank
     *            maximal acceptable rank. Must be positive.
     */
    public void setMaxAcceptableRank(int maxAcceptableRank) {
        if (maxAcceptableRank <= 0) {
            throw new IllegalArgumentException("maximal acceptable rank needs to be positive but was [" + maxAcceptableRank + "]");
        }
        this.maxAcceptableRank = maxAcceptableRank;
    }

    public int getMaxAcceptableRank() {
        return this.maxAcceptableRank;
    }

    /**
     * Compute ReciprocalRank based on provided relevant document IDs.
     * @return reciprocal Rank for above {@link SearchResult} list.
     **/
    @Override
    public EvalQueryQuality evaluate(SearchHit[] hits, List<RatedDocument> ratedDocs) {
        Set<RatedDocumentKey> relevantDocIds = new HashSet<>();
        Set<RatedDocumentKey> irrelevantDocIds = new HashSet<>();
        for (RatedDocument doc : ratedDocs) {
            if (Rating.RELEVANT.equals(RatingMapping.mapTo(doc.getRating()))) {
                relevantDocIds.add(doc.getKey());
            } else if (Rating.IRRELEVANT.equals(RatingMapping.mapTo(doc.getRating()))) {
                irrelevantDocIds.add(doc.getKey());
            }
        }

        Collection<RatedDocumentKey> unknownDocIds = new ArrayList<>();
        int firstRelevant = -1;
        boolean found = false;
        for (int i = 0; i < hits.length; i++) {
            // TODO here we use index/type/id triple not for a rated document but an unrated document in the search hits. Maybe rename?
            RatedDocumentKey id = new RatedDocumentKey(hits[i].getIndex(), hits[i].getType(), hits[i].getId());
            if (relevantDocIds.contains(id)) {
                if (found == false && i < maxAcceptableRank) {
                    firstRelevant = i + 1; // add one because rank is not
                                           // 0-based
                    found = true;
                }
            } else {
                unknownDocIds.add(id);
            }
        }

        double reciprocalRank = (firstRelevant == -1) ? 0 : 1.0d / firstRelevant;
        return new EvalQueryQuality(reciprocalRank, unknownDocIds);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(maxAcceptableRank);
    }

    private static final ParseField MAX_RANK_FIELD = new ParseField("max_acceptable_rank");
    private static final ObjectParser<ReciprocalRank, ParseFieldMatcherSupplier> PARSER = new ObjectParser<>(
            "reciprocal_rank", () -> new ReciprocalRank());

    static {
        PARSER.declareInt(ReciprocalRank::setMaxAcceptableRank, MAX_RANK_FIELD);
    }

    @Override
    public ReciprocalRank fromXContent(XContentParser parser, ParseFieldMatcher matcher) {
        return ReciprocalRank.fromXContent(parser, new ParseFieldMatcherSupplier() {
            
            @Override
            public ParseFieldMatcher getParseFieldMatcher() {
                return matcher;
            }
        });
    }

    public static ReciprocalRank fromXContent(XContentParser parser, ParseFieldMatcherSupplier matcher) {
        return PARSER.apply(parser, matcher);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(NAME);
        builder.field(MAX_RANK_FIELD.getPreferredName(), this.maxAcceptableRank);
        builder.endObject();
        builder.endObject();
        return builder;
    }
}
