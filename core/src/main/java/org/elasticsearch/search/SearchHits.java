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

package org.elasticsearch.search;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public final class SearchHits implements Streamable, ToXContentFragment, Iterable<SearchHit> {

    public static SearchHits empty() {
        // We shouldn't use static final instance, since that could directly be returned by native transport clients
        return new SearchHits(EMPTY, 0, 0);
    }

    public static final SearchHit[] EMPTY = new SearchHit[0];

    private SearchHit[] hits;

    public long totalHits;

    private float maxScore;

    SearchHits() {

    }

    public SearchHits(SearchHit[] hits, long totalHits, float maxScore) {
        this.hits = hits;
        this.totalHits = totalHits;
        this.maxScore = maxScore;
    }

    /**
     * The total number of hits that matches the search request.
     */
    public long getTotalHits() {
        return totalHits;
    }


    /**
     * The maximum score of this query.
     */
    public float getMaxScore() {
        return maxScore;
    }

    /**
     * The hits of the search request (based on the search type, and from / size provided).
     */
    public SearchHit[] getHits() {
        return this.hits;
    }

    /**
     * Return the hit as the provided position.
     */
    public SearchHit getAt(int position) {
        return hits[position];
    }

    @Override
    public Iterator<SearchHit> iterator() {
        return Arrays.stream(getHits()).iterator();
    }

    public static final class Fields {
        public static final String HITS = "hits";
        public static final String TOTAL = "total";
        public static final String MAX_SCORE = "max_score";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.HITS);
        builder.field(Fields.TOTAL, totalHits);
        if (Float.isNaN(maxScore)) {
            builder.nullField(Fields.MAX_SCORE);
        } else {
            builder.field(Fields.MAX_SCORE, maxScore);
        }
        builder.field(Fields.HITS);
        builder.startArray();
        for (SearchHit hit : hits) {
            hit.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static SearchHits fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            parser.nextToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        }
        XContentParser.Token token = parser.currentToken();
        String currentFieldName = null;
        List<SearchHit> hits = new ArrayList<>();
        long totalHits = 0;
        float maxScore = 0f;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (Fields.TOTAL.equals(currentFieldName)) {
                    totalHits = parser.longValue();
                } else if (Fields.MAX_SCORE.equals(currentFieldName)) {
                    maxScore = parser.floatValue();
                }
            } else if (token == XContentParser.Token.VALUE_NULL) {
                if (Fields.MAX_SCORE.equals(currentFieldName)) {
                    maxScore = Float.NaN; // NaN gets rendered as null-field
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (Fields.HITS.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        hits.add(SearchHit.fromXContent(parser));
                    }
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                parser.skipChildren();
            }
        }
        SearchHits searchHits = new SearchHits(hits.toArray(new SearchHit[hits.size()]), totalHits,
                maxScore);
        return searchHits;
    }


    public static SearchHits readSearchHits(StreamInput in) throws IOException {
        SearchHits hits = new SearchHits();
        hits.readFrom(in);
        return hits;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        final boolean hasTotalHits;
        if (in.getVersion().onOrAfter(Version.V_6_0_0_beta1)) {
            hasTotalHits = in.readBoolean();
        } else {
            hasTotalHits = true;
        }
        if (hasTotalHits) {
            totalHits = in.readVLong();
        } else {
            totalHits = -1;
        }
        maxScore = in.readFloat();
        int size = in.readVInt();
        if (size == 0) {
            hits = EMPTY;
        } else {
            hits = new SearchHit[size];
            for (int i = 0; i < hits.length; i++) {
                hits[i] = SearchHit.readSearchHit(in);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        final boolean hasTotalHits;
        if (out.getVersion().onOrAfter(Version.V_6_0_0_beta1)) {
            hasTotalHits = totalHits >= 0;
            out.writeBoolean(hasTotalHits);
        } else {
            assert totalHits >= 0;
            hasTotalHits = true;
        }
        if (hasTotalHits) {
            out.writeVLong(totalHits);
        }
        out.writeFloat(maxScore);
        out.writeVInt(hits.length);
        if (hits.length > 0) {
            for (SearchHit hit : hits) {
                hit.writeTo(out);
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SearchHits other = (SearchHits) obj;
        return Objects.equals(totalHits, other.totalHits)
                && Objects.equals(maxScore, other.maxScore)
                && Arrays.equals(hits, other.hits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalHits, maxScore, Arrays.hashCode(hits));
    }
}
