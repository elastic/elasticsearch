/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.SimpleRefCounted;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.transport.LeakTracker;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

public final class SearchHits implements Writeable, ChunkedToXContent, RefCounted, Iterable<SearchHit> {

    public static final SearchHit[] EMPTY = new SearchHit[0];
    public static final SearchHits EMPTY_WITH_TOTAL_HITS = SearchHits.empty(Lucene.TOTAL_HITS_EQUAL_TO_ZERO, 0);
    public static final SearchHits EMPTY_WITHOUT_TOTAL_HITS = SearchHits.empty(null, 0);

    private final SearchHit[] hits;
    private final TotalHits totalHits;
    private final float maxScore;
    @Nullable
    private final SortField[] sortFields;
    @Nullable
    private final String collapseField;
    @Nullable
    private final Object[] collapseValues;

    private final RefCounted refCounted;

    public static SearchHits empty(@Nullable TotalHits totalHits, float maxScore) {
        return new SearchHits(EMPTY, totalHits, maxScore);
    }

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
        this(
            hits,
            totalHits,
            maxScore,
            sortFields,
            collapseField,
            collapseValues,
            hits.length == 0 ? ALWAYS_REFERENCED : LeakTracker.wrap(new SimpleRefCounted())
        );
    }

    private SearchHits(
        SearchHit[] hits,
        @Nullable TotalHits totalHits,
        float maxScore,
        @Nullable SortField[] sortFields,
        @Nullable String collapseField,
        @Nullable Object[] collapseValues,
        RefCounted refCounted
    ) {
        this.hits = hits;
        this.totalHits = totalHits;
        this.maxScore = maxScore;
        this.sortFields = sortFields;
        this.collapseField = collapseField;
        this.collapseValues = collapseValues;
        this.refCounted = refCounted;
    }

    public static SearchHits unpooled(SearchHit[] hits, @Nullable TotalHits totalHits, float maxScore) {
        return unpooled(hits, totalHits, maxScore, null, null, null);
    }

    public static SearchHits unpooled(
        SearchHit[] hits,
        @Nullable TotalHits totalHits,
        float maxScore,
        @Nullable SortField[] sortFields,
        @Nullable String collapseField,
        @Nullable Object[] collapseValues
    ) {
        assert assertUnpooled(hits);
        return new SearchHits(hits, totalHits, maxScore, sortFields, collapseField, collapseValues, ALWAYS_REFERENCED);
    }

    private static boolean assertUnpooled(SearchHit[] searchHits) {
        for (SearchHit searchHit : searchHits) {
            assert searchHit.isPooled() == false : "hit was pooled [" + searchHit + "]";
        }
        return true;
    }

    public static SearchHits readFrom(StreamInput in, boolean pooled) throws IOException {
        final TotalHits totalHits;
        if (in.readBoolean()) {
            totalHits = Lucene.readTotalHits(in);
        } else {
            // track_total_hits is false
            totalHits = null;
        }
        final float maxScore = in.readFloat();
        int size = in.readVInt();
        final SearchHit[] hits;
        boolean isPooled = false;
        if (size == 0) {
            hits = EMPTY;
        } else {
            hits = new SearchHit[size];
            for (int i = 0; i < hits.length; i++) {
                var hit = SearchHit.readFrom(in, pooled);
                hits[i] = hit;
                isPooled = isPooled || hit.isPooled();
            }
        }
        var sortFields = in.readOptional(Lucene::readSortFieldArray);
        var collapseField = in.readOptionalString();
        var collapseValues = in.readOptional(Lucene::readSortValues);
        if (isPooled) {
            return new SearchHits(hits, totalHits, maxScore, sortFields, collapseField, collapseValues);
        } else {
            return unpooled(hits, totalHits, maxScore, sortFields, collapseField, collapseValues);
        }
    }

    public boolean isPooled() {
        return refCounted != ALWAYS_REFERENCED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert hasReferences();
        final boolean hasTotalHits = totalHits != null;
        out.writeBoolean(hasTotalHits);
        if (hasTotalHits) {
            Lucene.writeTotalHits(out, totalHits);
        }
        out.writeFloat(maxScore);
        out.writeArray(hits);
        out.writeOptional(Lucene::writeSortFieldArray, sortFields);
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
        assert hasReferences();
        return this.hits;
    }

    /**
     * Return the hit as the provided position.
     */
    public SearchHit getAt(int position) {
        assert hasReferences();
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
        assert hasReferences();
        return Iterators.forArray(getHits());
    }

    @Override
    public void incRef() {
        refCounted.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return refCounted.tryIncRef();
    }

    @Override
    public boolean decRef() {
        if (refCounted.decRef()) {
            deallocate();
            return true;
        }
        return false;
    }

    private void deallocate() {
        var hits = this.hits;
        for (int i = 0; i < hits.length; i++) {
            assert hits[i] != null;
            hits[i].decRef();
            hits[i] = null;
        }
    }

    @Override
    public boolean hasReferences() {
        return refCounted.hasReferences();
    }

    public SearchHits asUnpooled() {
        assert hasReferences();
        if (refCounted == ALWAYS_REFERENCED) {
            return this;
        }
        final SearchHit[] unpooledHits = new SearchHit[hits.length];
        for (int i = 0; i < hits.length; i++) {
            unpooledHits[i] = hits[i].asUnpooled();
        }
        return unpooled(unpooledHits, totalHits, maxScore, sortFields, collapseField, collapseValues);
    }

    public static final class Fields {
        public static final String HITS = "hits";
        public static final String TOTAL = "total";
        public static final String MAX_SCORE = "max_score";
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        assert hasReferences();
        return Iterators.concat(Iterators.single((b, p) -> {
            b.startObject(Fields.HITS);
            boolean totalHitAsInt = params.paramAsBoolean(RestSearchAction.TOTAL_HITS_AS_INT_PARAM, false);
            if (totalHitAsInt) {
                long total = totalHits == null ? -1 : totalHits.value();
                b.field(Fields.TOTAL, total);
            } else if (totalHits != null) {
                b.startObject(Fields.TOTAL);
                b.field("value", totalHits.value());
                b.field("relation", totalHits.relation() == Relation.EQUAL_TO ? "eq" : "gte");
                b.endObject();
            }
            if (Float.isNaN(maxScore)) {
                b.nullField(Fields.MAX_SCORE);
            } else {
                b.field(Fields.MAX_SCORE, maxScore);
            }
            return b;
        }), ChunkedToXContentHelper.array(Fields.HITS, Iterators.forArray(hits)), ChunkedToXContentHelper.endObject());
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
