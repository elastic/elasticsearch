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

import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.action.search.RestSearchAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public final class SearchHits implements Streamable, ToXContentFragment, Iterable<SearchHit> {
    public static SearchHits empty() {
        return empty(true);
    }

    public static SearchHits empty(boolean withTotalHits) {
        // We shouldn't use static final instance, since that could directly be returned by native transport clients
        return new SearchHits(EMPTY, withTotalHits ? new TotalHits(0, Relation.EQUAL_TO) : null, 0);
    }

    public static final SearchHit[] EMPTY = new SearchHit[0];

    private SearchHit[] hits;

    private Total totalHits;

    private float maxScore;

    @Nullable
    private SortField[] sortFields;
    @Nullable
    private String collapseField;
    @Nullable
    private Object[] collapseValues;

    SearchHits() {

    }

    public SearchHits(SearchHit[] hits, @Nullable TotalHits totalHits, float maxScore) {
        this(hits, totalHits, maxScore, null, null, null);
    }

    public SearchHits(SearchHit[] hits, @Nullable TotalHits totalHits, float maxScore, @Nullable SortField[] sortFields,
                      @Nullable String collapseField, @Nullable Object[] collapseValues) {
        this.hits = hits;
        this.totalHits = totalHits == null ? null : new Total(totalHits);
        this.maxScore = maxScore;
        this.sortFields = sortFields;
        this.collapseField = collapseField;
        this.collapseValues = collapseValues;
    }

    /**
     * The total number of hits for the query or null if the tracking of total hits
     * is disabled in the request.
     */
    public TotalHits getTotalHits() {
        return totalHits == null ? null : totalHits.in;
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

    /**
     * In case documents were sorted by field(s), returns information about such field(s), null otherwise
     * @see SortField
     */
    @Nullable
    public SortField[] getSortFields() {
        return sortFields;
    }

    /**
     * In case field collapsing was performed, returns the field used for field collapsing, null otherwise
     */
    @Nullable
    public String getCollapseField() {
        return collapseField;
    }

    /**
     * In case field collapsing was performed, returns the values of the field that field collapsing was performed on, null otherwise
     */
    @Nullable
    public Object[] getCollapseValues() {
        return collapseValues;
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
        boolean totalHitAsInt = params.paramAsBoolean(RestSearchAction.TOTAL_HITS_AS_INT_PARAM, false);
        if (totalHitAsInt) {
            long total = totalHits == null ? -1 : totalHits.in.value;
            builder.field(Fields.TOTAL, total);
        } else if (totalHits != null) {
            builder.startObject(Fields.TOTAL);
            totalHits.toXContent(builder, params);
            builder.endObject();
        }
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
        TotalHits totalHits = null;
        float maxScore = 0f;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (Fields.TOTAL.equals(currentFieldName)) {
                    // For BWC with nodes pre 7.0
                    long value = parser.longValue();
                    totalHits = value == -1 ? null : new TotalHits(value, Relation.EQUAL_TO);
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
                if (Fields.TOTAL.equals(currentFieldName)) {
                    totalHits = parseTotalHitsFragment(parser);
                } else {
                    parser.skipChildren();
                }
            }
        }
        return new SearchHits(hits.toArray(new SearchHit[0]), totalHits, maxScore);
    }

    public static SearchHits readSearchHits(StreamInput in) throws IOException {
        SearchHits hits = new SearchHits();
        hits.readFrom(in);
        return hits;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            totalHits = new Total(in);
        } else {
            // track_total_hits is false
            totalHits = null;
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
        if (in.getVersion().onOrAfter(Version.V_6_6_0)) {
            sortFields = in.readOptionalArray(Lucene::readSortField, SortField[]::new);
            collapseField = in.readOptionalString();
            collapseValues = in.readOptionalArray(Lucene::readSortValue, Object[]::new);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        final boolean hasTotalHits = totalHits != null;
        out.writeBoolean(hasTotalHits);
        if (hasTotalHits) {
            totalHits.writeTo(out);
        }
        out.writeFloat(maxScore);
        out.writeVInt(hits.length);
        if (hits.length > 0) {
            for (SearchHit hit : hits) {
                hit.writeTo(out);
            }
        }
        if (out.getVersion().onOrAfter(Version.V_6_6_0)) {
            out.writeOptionalArray(Lucene::writeSortField, sortFields);
            out.writeOptionalString(collapseField);
            out.writeOptionalArray(Lucene::writeSortValue, collapseValues);
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
                && Arrays.equals(hits, other.hits)
                && Arrays.equals(sortFields, other.sortFields)
                && Objects.equals(collapseField, other.collapseField)
                && Arrays.equals(collapseValues, other.collapseValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalHits, maxScore, Arrays.hashCode(hits),
            Arrays.hashCode(sortFields), collapseField, Arrays.hashCode(collapseValues));
    }

    public static TotalHits parseTotalHitsFragment(XContentParser parser) throws IOException {
        long value = -1;
        Relation relation = null;
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("value".equals(currentFieldName)) {
                    value = parser.longValue();
                } else if ("relation".equals(currentFieldName)) {
                    relation = parseRelation(parser.text());
                }
            } else {
                parser.skipChildren();
            }
        }
        return new TotalHits(value, relation);
    }

    private static Relation parseRelation(String relation) {
        if ("gte".equals(relation)) {
            return Relation.GREATER_THAN_OR_EQUAL_TO;
        } else if ("eq".equals(relation)) {
            return Relation.EQUAL_TO;
        } else {
            throw new IllegalArgumentException("invalid total hits relation: " + relation);
        }
    }

    private static String printRelation(Relation relation) {
        return relation == Relation.EQUAL_TO ? "eq" : "gte";
    }

    private static class Total implements Writeable, ToXContentFragment {
        final TotalHits in;

        Total(TotalHits in) {
            this.in = Objects.requireNonNull(in);
        }

        Total(StreamInput in) throws IOException {
            this.in = Lucene.readTotalHits(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            Lucene.writeTotalHits(out, in);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Total total = (Total) o;
            return in.value == total.in.value &&
                in.relation == total.in.relation;
        }

        @Override
        public int hashCode() {
            return Objects.hash(in.value, in.relation);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("value", in.value);
            builder.field("relation", printRelation(in.relation));
            return builder;
        }
    }
}
