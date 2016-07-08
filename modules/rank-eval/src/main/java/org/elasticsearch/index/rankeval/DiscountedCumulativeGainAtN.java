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
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DiscountedCumulativeGainAtN extends RankedListQualityMetric {

    /** Number of results to check against a given set of relevant results. */
    private int n;

    public static final String NAME = "dcg_at_n";
    private static final double LOG2 = Math.log(2.0);

    public DiscountedCumulativeGainAtN(StreamInput in) throws IOException {
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
    public DiscountedCumulativeGainAtN() {
        this.n = 10;
    }

    /**
     * @param n number of top results to check against a given set of relevant results. Must be positive.
     */
    public DiscountedCumulativeGainAtN(int n) {
        if (n <= 0) {
            throw new IllegalArgumentException("number of results to check needs to be positive but was " + n);
        }
        this.n = n;
    }

    /**
     * Return number of search results to check for quality.
     */
    public int getN() {
        return n;
    }

    @Override
    public EvalQueryQuality evaluate(SearchHit[] hits, List<RatedDocument> ratedDocs) {
        Map<String, RatedDocument> ratedDocsById = new HashMap<>();
        for (RatedDocument doc : ratedDocs) {
            ratedDocsById.put(doc.getDocID(), doc);
        }

        Collection<String> unknownDocIds = new ArrayList<String>();
        double dcg = 0;

        for (int i = 0; (i < n && i < hits.length); i++) {
            int rank = i + 1; // rank is 1-based
            String id = hits[i].getId();
            RatedDocument ratedDoc = ratedDocsById.get(id);
            if (ratedDoc != null) {
                int rel = ratedDoc.getRating();
                dcg += (Math.pow(2, rel) - 1) / ((Math.log(rank + 1) / LOG2));
            } else {
                unknownDocIds.add(id);
            }
        }
        return new EvalQueryQuality(dcg, unknownDocIds);
    }

    private static final ParseField SIZE_FIELD = new ParseField("size");
    private static final ConstructingObjectParser<DiscountedCumulativeGainAtN, ParseFieldMatcherSupplier> PARSER =
            new ConstructingObjectParser<>("dcg_at", a -> new DiscountedCumulativeGainAtN((Integer) a[0]));

    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), SIZE_FIELD);
    }

    public static DiscountedCumulativeGainAtN fromXContent(XContentParser parser, ParseFieldMatcherSupplier matcher) {
        return PARSER.apply(parser, matcher);
    }
}
