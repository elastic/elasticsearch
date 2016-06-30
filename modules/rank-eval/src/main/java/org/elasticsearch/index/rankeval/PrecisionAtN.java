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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import javax.naming.directory.SearchResult;

/**
 * Evaluate Precision at N, N being the number of search results to consider for precision calculation.
 * 
 * Documents of unkonwn quality are ignored in the precision at n computation and returned by document id.
 * */
public class PrecisionAtN implements RankedListQualityMetric {
    
    /** Number of results to check against a given set of relevant results. */
    private int n;
    
    public static final String NAME = "precisionatn";

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

    /** Compute precisionAtN based on provided relevant document IDs.
     * @return precision at n for above {@link SearchResult} list.
     **/
    @Override
    public EvalQueryQuality evaluate(SearchHit[] hits, RatedQuery intent) {
        Map<String, Integer> ratedDocIds = intent.getRatedDocuments();
        
        Collection<String> relevantDocIds = new ArrayList<>(); 
        for (Entry<String, Integer> entry : ratedDocIds.entrySet()) {
            if (Rating.RELEVANT.equals(RatingMapping.mapTo(entry.getValue()))) {
                relevantDocIds.add(entry.getKey());
            }
        }
        
        Collection<String> irrelevantDocIds = new ArrayList<>(); 
        for (Entry<String, Integer> entry : ratedDocIds.entrySet()) {
            if (Rating.IRRELEVANT.equals(RatingMapping.mapTo(entry.getValue()))) {
                irrelevantDocIds.add(entry.getKey());
            }
        }

        int good = 0;
        int bad = 0;
        Collection<String> unknownDocIds = new ArrayList<String>();
        for (int i = 0; (i < n && i < hits.length); i++) {
            String id = hits[i].getId();
            if (relevantDocIds.contains(id)) {
                good++;
            } else if (irrelevantDocIds.contains(id)) {
                bad++;
            } else {
                unknownDocIds.add(id);
            }
        }

        double precision = (double) good / (good + bad);

        return new EvalQueryQuality(precision, unknownDocIds);
    }
    
    public enum Rating {
        RELEVANT, IRRELEVANT;
    }
    
    /**
     * Needed to get the enum accross serialisation boundaries.
     * */
    public static class RatingMapping {
        public static Integer mapFrom(Rating rating) {
            if (Rating.RELEVANT.equals(rating)) {
                return 0;
            }
            return 1;
        }
        
        public static Rating mapTo(Integer rating) {
            if (rating == 0) {
                return Rating.RELEVANT;
            }
            return Rating.IRRELEVANT;
        }
    }

}
