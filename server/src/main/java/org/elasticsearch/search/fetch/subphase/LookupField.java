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
 * A {@link LookupField} is an **unresolved** fetch field whose values will be resolved later.
 * The fetch phase and top-hits aggregations in the query phase create unresolved lookup fields
 * for each search hit and store them in {@link DocumentField#getLookupFields()}.
 *
 * @see org.elasticsearch.index.mapper.LookupRuntimeFieldType
 * @see FetchLookupFieldsPhase
 */
public final class LookupField implements Writeable {

    private final String lookupIndex;
    private final QueryBuilder query;
    private final List<FieldAndFormat> fetchFields;

    public LookupField(String lookupIndex, QueryBuilder query, List<FieldAndFormat> fetchFields) {
        this.lookupIndex = lookupIndex;
        this.query = query;
        this.fetchFields = fetchFields;
    }

    public LookupField(StreamInput in) throws IOException {
        this.lookupIndex = in.readString();
        this.query = in.readNamedWriteable(QueryBuilder.class);
        this.fetchFields = in.readList(FieldAndFormat::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(lookupIndex);
        out.writeNamedWriteable(query);
        out.writeCollection(fetchFields);
    }

    public SearchRequest toSearchRequest() {
        final SearchSourceBuilder source = new SearchSourceBuilder().query(query).trackScores(false).size(1).fetchSource(false);
        fetchFields.forEach(source::fetchField);
        return new SearchRequest().indices(lookupIndex).source(source);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final LookupField that = (LookupField) o;
        return lookupIndex.equals(that.lookupIndex) && query.equals(that.query) && fetchFields.equals(that.fetchFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lookupIndex, query, fetchFields);
    }

    @Override
    public String toString() {
        return "LookupField{" + "index='" + lookupIndex + '\'' + ", query=" + query + ", fetchFields=" + fetchFields + '}';
    }
}
