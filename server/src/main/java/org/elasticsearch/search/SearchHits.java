/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public final class SearchHits implements Writeable, ChunkedToXContent, Iterable<SearchHit> {

    public static final SearchHit[] EMPTY = new SearchHit[0];
    public static final SearchHits EMPTY_WITH_TOTAL_HITS = new SearchHits(EMPTY, new TotalHits(0, Relation.EQUAL_TO), 0);
    public static final SearchHits EMPTY_WITHOUT_TOTAL_HITS = new SearchHits(EMPTY, null, 0);

    private final SearchHit[] hits;
    private final TotalHits totalHits;
    private final float maxScore;
    @Nullable
    private final SortField[] sortFields;
    @Nullable
    private final String collapseField;
    @Nullable
    private final Object[] collapseValues;

    public SearchHits(SearchHit[] hits, @Nullable TotalHits totalHits, float maxScore) {
        this(hits, totalHits, maxScore, null, null, null);
    }

    public SearchHits(
        SearchHit[] hits,
        @Nullable TotalHits totalHits,
        float maxScore,
        @Nullable SortField[] sortFields,
        @Nullable String collapseField,
        @Nullable Object[] collapseValues
    ) {
        this.hits = hits;
        this.totalHits = totalHits;
        this.maxScore = maxScore;
        this.sortFields = sortFields;
        this.collapseField = collapseField;
        this.collapseValues = collapseValues;
    }

    public SearchHits(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            totalHits = Lucene.readTotalHits(in);
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
                hits[i] = new SearchHit(in);
            }
        }
        sortFields = in.readOptionalArray(Lucene::readSortField, SortField[]::new);
        collapseField = in.readOptionalString();
        collapseValues = in.readOptionalArray(Lucene::readSortValue, Object[]::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        final boolean hasTotalHits = totalHits != null;
        out.writeBoolean(hasTotalHits);
        if (hasTotalHits) {
            Lucene.writeTotalHits(out, totalHits);
        }
        out.writeFloat(maxScore);
        out.writeArray(hits);
        out.writeOptionalArray(Lucene::writeSortField, sortFields);
        out.writeOptionalString(collapseField);
        out.writeOptionalArray(Lucene::writeSortValue, collapseValues);
    }

    /**
     * The total number of hits for the query or null if the tracking of total hits
     * is disabled in the request.
     */
    @Nullable
    public TotalHits getTotalHits() {
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
        return Iterators.forArray(getHits());
    }

    public static final class Fields {
        public static final String HITS = "hits";
        public static final String TOTAL = "total";
        public static final String MAX_SCORE = "max_score";
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(Iterators.single((b, p) -> b.startObject(Fields.HITS)), Iterators.single((b, p) -> {
            boolean totalHitAsInt = params.paramAsBoolean(RestSearchAction.TOTAL_HITS_AS_INT_PARAM, false);
            if (totalHitAsInt) {
                long total = totalHits == null ? -1 : totalHits.value;
                b.field(Fields.TOTAL, total);
            } else if (totalHits != null) {
                b.startObject(Fields.TOTAL);
                b.field("value", totalHits.value);
                b.field("relation", totalHits.relation == Relation.EQUAL_TO ? "eq" : "gte");
                b.endObject();
            }
            return b;
        }), Iterators.single((b, p) -> {
            if (Float.isNaN(maxScore)) {
                b.nullField(Fields.MAX_SCORE);
            } else {
                b.field(Fields.MAX_SCORE, maxScore);
            }
            return b;
        }), ChunkedToXContentHelper.array(Fields.HITS, Iterators.forArray(hits)), ChunkedToXContentHelper.endObject());
    }

    public static SearchHits fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            parser.nextToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
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
        return Objects.hash(
            totalHits,
            maxScore,
            Arrays.hashCode(hits),
            Arrays.hashCode(sortFields),
            collapseField,
            Arrays.hashCode(collapseValues)
        );
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
}
