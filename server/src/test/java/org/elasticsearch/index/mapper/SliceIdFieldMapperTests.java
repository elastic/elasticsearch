/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.mapper.IdFieldMapper.AbstractIdFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests the {@code _id} field type used by slice-enabled indices. The index carries two {@code _id} terms per doc
 * (a slice-free search term and a compound identity term); {@code ids}/{@code term} search seeks the search term and
 * is therefore slice-context-free. The field type is exercised directly via {@link SliceIdFieldMapper#INSTANCE}.
 */
public class SliceIdFieldMapperTests extends MapperServiceTestCase {

    private static AbstractIdFieldType fieldType() {
        return (AbstractIdFieldType) SliceIdFieldMapper.INSTANCE.fieldType();
    }

    public void testStoredIdDecodesPlain() {
        // The stored _id is the plain encodeId(id), so the default decodeStoredId returns the user-visible id directly.
        String id = randomAlphaOfLengthBetween(1, 16);
        assertThat(fieldType().decodeStoredId(Uid.encodeId(id)), equalTo(id));
    }

    public void testFielddataIsNotSupported() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> fieldType().fielddataBuilder(FieldDataContext.noRuntimeFields("test", "test"))
        );
        assertThat(e.getMessage(), containsString("Fielddata is not supported on [_id] field in slice-enabled indices"));
    }

    public void testTermsQuerySeeksSearchTermSliceContextFree() throws Exception {
        // No slice routing on the context: id search must still work (the search term is derived only from the id).
        SearchExecutionContext context = createSearchExecutionContext(createMapperService(mapping(b -> {})));
        Query query = fieldType().termsQuery(List.of("a", "b"), context);
        Query expected = new TermInSetQuery(IdFieldMapper.NAME, List.of(Uid.searchTerm("a"), Uid.searchTerm("b")));
        assertThat(query, equalTo(expected));
    }

    public void testTermQueryDelegatesToTermsQuery() throws Exception {
        SearchExecutionContext context = createSearchExecutionContext(createMapperService(mapping(b -> {})));
        Query query = fieldType().termQuery("a", context);
        Query expected = new TermInSetQuery(IdFieldMapper.NAME, List.of(Uid.searchTerm("a")));
        assertThat(query, equalTo(expected));
    }
}
