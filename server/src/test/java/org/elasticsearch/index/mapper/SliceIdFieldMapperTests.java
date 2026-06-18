/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.mapper.IdFieldMapper.AbstractIdFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

/**
 * Tests the {@code _id} field type used by slice-enabled indices. The index carries two {@code _id} terms per doc
 * (a slice-free search term and a compound identity term); {@code ids}/{@code term} search seeks the search term and
 * is therefore slice-context-free. The field type is exercised directly via {@link SliceIdFieldMapper#DOCUMENT}.
 */
public class SliceIdFieldMapperTests extends MapperServiceTestCase {

    private static AbstractIdFieldType fieldType() {
        return (AbstractIdFieldType) SliceIdFieldMapper.DOCUMENT.fieldType();
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

    public void testDocumentModeStoresPlainId() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexSettings.SLICE_ENABLED.getKey(), true)
            .put(IndexSettings.SLICE_VALIDATED.getKey(), true)
            .build();
        MapperService mapperService = createMapperService(settings, mapping(b -> {}));
        assertFalse(sliceIdMapper(mapperService).isColumnarMode());

        String id = randomAlphaOfLengthBetween(1, 16);
        ParsedDocument doc = mapperService.documentMapper().parse(source(id, b -> {}, "slice-1"));
        List<IndexableField> idFields = doc.rootDoc().getFields(IdFieldMapper.NAME);
        // Two indexed terms (search + compound) plus a stored field carrying the plain id; no doc values.
        assertThat(idFields, hasItem(storedField(Uid.encodeId(id))));
        assertThat(idFields, hasItem(indexedTerm(Uid.searchTerm(id))));
        assertThat(idFields, hasItem(indexedTerm(Uid.encodeCompoundId(id, "slice-1"))));
        for (IndexableField f : idFields) {
            assertThat("document mode must not use doc values", f.fieldType().docValuesType(), equalTo(DocValuesType.NONE));
        }
    }

    public void testColumnarModeStoresPlainIdInBinaryDocValues() throws Exception {
        assumeTrue(
            "columnar _id requires the extended doc values feature flag",
            FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled()
        );
        Settings settings = Settings.builder()
            .put(IndexSettings.SLICE_ENABLED.getKey(), true)
            .put(IndexSettings.SLICE_VALIDATED.getKey(), true)
            .put(IndexSettings.USE_COLUMNAR_ID_BY_DEFAULT.getKey(), true)
            .build();
        MapperService mapperService = createMapperService(settings, mapping(b -> {}));
        assertTrue("columnar default should make the slice _id mapper columnar", sliceIdMapper(mapperService).isColumnarMode());

        String id = randomAlphaOfLengthBetween(1, 16);
        ParsedDocument doc = mapperService.documentMapper().parse(source(id, b -> {}, "slice-1"));
        List<IndexableField> idFields = doc.rootDoc().getFields(IdFieldMapper.NAME);
        // The same two indexed terms (search + compound) but the plain id is in binary doc values, not a stored field.
        assertThat(idFields, hasItem(indexedTerm(Uid.searchTerm(id))));
        assertThat(idFields, hasItem(indexedTerm(Uid.encodeCompoundId(id, "slice-1"))));
        IndexableField docValues = null;
        for (IndexableField f : idFields) {
            assertFalse("columnar mode must not store _id", f.fieldType().stored());
            if (f.fieldType().docValuesType() == DocValuesType.BINARY) {
                docValues = f;
            }
        }
        assertNotNull("columnar mode must keep the plain id in binary doc values", docValues);
        assertThat(docValues.binaryValue(), equalTo(Uid.encodeId(id)));
    }

    private static SliceIdFieldMapper sliceIdMapper(MapperService mapperService) {
        return (SliceIdFieldMapper) mapperService.mappingLookup().getMapping().getMetadataMapperByName(IdFieldMapper.NAME);
    }

    private static org.hamcrest.Matcher<IndexableField> storedField(BytesRef value) {
        return new org.hamcrest.TypeSafeMatcher<>() {
            @Override
            protected boolean matchesSafely(IndexableField f) {
                return f.fieldType().stored() && value.equals(f.binaryValue());
            }

            @Override
            public void describeTo(org.hamcrest.Description description) {
                description.appendText("a stored _id field with value ").appendValue(value);
            }
        };
    }

    private static org.hamcrest.Matcher<IndexableField> indexedTerm(BytesRef term) {
        return new org.hamcrest.TypeSafeMatcher<>() {
            @Override
            protected boolean matchesSafely(IndexableField f) {
                return f.fieldType().indexOptions() != IndexOptions.NONE && f.fieldType().stored() == false && term.equals(f.binaryValue());
            }

            @Override
            public void describeTo(org.hamcrest.Description description) {
                description.appendText("an indexed _id term ").appendValue(term);
            }
        };
    }
}
