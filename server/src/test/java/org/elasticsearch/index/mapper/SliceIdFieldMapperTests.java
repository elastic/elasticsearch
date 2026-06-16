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
 * Tests the {@code _id} field type used by slice-enabled indices, where {@code _id} is stored as a composite
 * {@code (slice, id)} term. The field type is exercised directly: its behaviour does not depend on the slice
 * feature flag, only on the composite encoding implemented by {@link Uid}.
 */
public class SliceIdFieldMapperTests extends MapperServiceTestCase {

    private static AbstractIdFieldType fieldType() {
        return (AbstractIdFieldType) SliceIdFieldMapper.INSTANCE.fieldType();
    }

    public void testDecodeStoredIdRecoversPlainId() {
        String id = randomAlphaOfLengthBetween(1, 16);
        String slice = randomAlphaOfLengthBetween(1, 16);
        // The stored value is the composite uid; decodeStoredId must hand back the user-visible id.
        assertThat(fieldType().decodeStoredId(Uid.encodeSliceId(slice, id)), equalTo(id));
    }

    public void testFielddataIsNotSupported() {
        // The composite term ordinals are meaningless, so fielddata must be rejected rather than silently producing garbage.
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> fieldType().fielddataBuilder(FieldDataContext.noRuntimeFields("test", "test"))
        );
        assertThat(e.getMessage(), containsString("Fielddata is not supported on [_id] field in slice-enabled indices"));
    }

    public void testTermsQueryBuildsCompositeTermsForConcreteSlice() throws Exception {
        SearchExecutionContext context = createSearchExecutionContext(createMapperService(mapping(b -> {})));
        context.setSliceRouting("slice-1");
        Query query = fieldType().termsQuery(List.of("a", "b"), context);
        Query expected = new TermInSetQuery(
            IdFieldMapper.NAME,
            List.of(Uid.encodeSliceId("slice-1", "a"), Uid.encodeSliceId("slice-1", "b"))
        );
        assertThat(query, equalTo(expected));
    }

    public void testTermsQueryExpandsAcrossMultipleSlices() throws Exception {
        SearchExecutionContext context = createSearchExecutionContext(createMapperService(mapping(b -> {})));
        context.setSliceRouting("slice-1,slice-2");
        Query query = fieldType().termsQuery(List.of("a"), context);
        Query expected = new TermInSetQuery(
            IdFieldMapper.NAME,
            List.of(Uid.encodeSliceId("slice-1", "a"), Uid.encodeSliceId("slice-2", "a"))
        );
        assertThat(query, equalTo(expected));
    }

    public void testTermQueryDelegatesToTermsQuery() throws Exception {
        SearchExecutionContext context = createSearchExecutionContext(createMapperService(mapping(b -> {})));
        context.setSliceRouting("slice-1");
        Query query = fieldType().termQuery("a", context);
        Query expected = new TermInSetQuery(IdFieldMapper.NAME, List.of(Uid.encodeSliceId("slice-1", "a")));
        assertThat(query, equalTo(expected));
    }

    public void testTermsQueryRequiresConcreteSlice() throws Exception {
        SearchExecutionContext context = createSearchExecutionContext(createMapperService(mapping(b -> {})));
        // No slice routing set, which corresponds to a cross-slice (_all) query; that is not supported for id/terms queries.
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> fieldType().termsQuery(List.of("a"), context));
        assertThat(e.getMessage(), containsString("queries require a concrete [_slice]"));
    }
}
