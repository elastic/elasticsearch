/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A {@link LookupField} is an *unresolved* fetch field whose values will be resolved later.
 * Data nodes create unresolved lookup fields for each search hit and stored them in {@link DocumentField#getLookupFields()}
 * during the fetch phase and coordinating nodes resolve them using {@link FetchLookupFieldsPhase}.
 *
 * @see org.elasticsearch.index.mapper.LookupRuntimeFieldType
 */
public final class LookupField implements Writeable {

    private final String index;
    private final QueryBuilder query;
    private final List<FieldAndFormat> fetchFields;
    private final int maxSize;

    public LookupField(String index, QueryBuilder query, List<FieldAndFormat> fetchFields, int maxSize) {
        this.index = index;
        this.query = query;
        this.maxSize = maxSize;
        this.fetchFields = fetchFields;
    }

    public LookupField(StreamInput in) throws IOException {
        this.index = in.readString();
        this.query = in.readNamedWriteable(QueryBuilder.class);
        this.fetchFields = in.readList(FieldAndFormat::new);
        this.maxSize = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeNamedWriteable(query);
        out.writeCollection(fetchFields);
        out.writeVInt(maxSize);
    }

    public SearchRequest toSearchRequest() {
        final SearchSourceBuilder source = new SearchSourceBuilder().query(query).size(maxSize).fetchSource(false);
        fetchFields.forEach(source::fetchField);
        return new SearchRequest().indices(index).source(source);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final LookupField that = (LookupField) o;
        return maxSize == that.maxSize && index.equals(that.index) && query.equals(that.query) && fetchFields.equals(that.fetchFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, query, maxSize, fetchFields);
    }

    @Override
    public String toString() {
        return "LookupField{"
            + "index='"
            + index
            + '\''
            + ", query="
            + query
            + ", maxSize="
            + maxSize
            + ", fetchFields="
            + fetchFields
            + '}';
    }
}
