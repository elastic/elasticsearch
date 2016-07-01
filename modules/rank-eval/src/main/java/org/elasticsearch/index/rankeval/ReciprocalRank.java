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

import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
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
public class ReciprocalRank extends RankedListQualityMetric {

    public static final String NAME = "reciprocal_rank";
    // the rank to use if the result list does not contain any relevant document
    // TODO decide on better default or make configurable
    private static final int RANK_IF_NOT_FOUND = Integer.MAX_VALUE;

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * Compute ReciprocalRank based on provided relevant document IDs.
     * @return reciprocal Rank for above {@link SearchResult} list.
     **/
    @Override
    public EvalQueryQuality evaluate(SearchHit[] hits, List<RatedDocument> ratedDocs) {
        Set<String> relevantDocIds = new HashSet<>();
        Set<String> irrelevantDocIds = new HashSet<>();
        for (RatedDocument doc : ratedDocs) {
            if (Rating.RELEVANT.equals(RatingMapping.mapTo(doc.getRating()))) {
                relevantDocIds.add(doc.getDocID());
            } else if (Rating.IRRELEVANT.equals(RatingMapping.mapTo(doc.getRating()))) {
                irrelevantDocIds.add(doc.getDocID());
            }
        }

        Collection<String> unknownDocIds = new ArrayList<String>();
        int firstRelevant = RANK_IF_NOT_FOUND;
        boolean found = false;
        for (int i = 0; i < hits.length; i++) {
            String id = hits[i].getId();
            if (relevantDocIds.contains(id) && found == false) {
                firstRelevant = i + 1; // add one because rank is not 0-based
                found = true;
                continue;
            } else if (irrelevantDocIds.contains(id) == false) {
                unknownDocIds.add(id);
            }
        }

        double reciprocalRank = 1.0d / firstRelevant;
        return new EvalQueryQuality(reciprocalRank, unknownDocIds);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
    }

    private static final ObjectParser<ReciprocalRank, ParseFieldMatcherSupplier> PARSER = new ObjectParser<>(
            "reciprocal_rank", () -> new ReciprocalRank());

    public static ReciprocalRank fromXContent(XContentParser parser, ParseFieldMatcherSupplier matcher) {
        return PARSER.apply(parser, matcher);
    }
}
