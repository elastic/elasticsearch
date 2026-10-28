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
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.mapper.IdFieldMapper.AbstractIdFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
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
        Query expected = new TermInSetQuery(
            IdFieldMapper.NAME,
            List.of(SliceIdFieldMapper.searchTerm("a"), SliceIdFieldMapper.searchTerm("b"))
        );
        assertThat(query, equalTo(expected));
    }

    public void testTermQueryDelegatesToTermsQuery() throws Exception {
        SearchExecutionContext context = createSearchExecutionContext(createMapperService(mapping(b -> {})));
        Query query = fieldType().termQuery("a", context);
        Query expected = new TermInSetQuery(IdFieldMapper.NAME, List.of(SliceIdFieldMapper.searchTerm("a")));
        assertThat(query, equalTo(expected));
    }

    public void testDocumentModeStoresCompoundId() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder().put(IndexSettings.SLICE_ENABLED.getKey(), true).build();
        MapperService mapperService = createMapperService(settings, mapping(b -> {}));
        assertFalse(sliceIdMapper(mapperService).isColumnarMode());

        String id = randomAlphaOfLengthBetween(1, 16);
        ParsedDocument doc = mapperService.documentMapper().parse(source(id, b -> {}, "slice-1"));
        List<IndexableField> idFields = doc.rootDoc().getFields(IdFieldMapper.NAME);
        // Two indexed terms (search + compound) plus a stored field carrying the compound id; no doc values.
        // The compound bytes are also the stored value, matching tombstone storage and eliminating live-vs-tombstone
        // branching in the engine/recovery paths.
        assertThat(idFields, hasItem(storedField(SliceIdFieldMapper.encodeCompoundId(id, "slice-1"))));
        assertThat(idFields, hasItem(indexedTerm(SliceIdFieldMapper.searchTerm(id))));
        assertThat(idFields, hasItem(indexedTerm(SliceIdFieldMapper.encodeCompoundId(id, "slice-1"))));
        for (IndexableField f : idFields) {
            assertThat("document mode must not use doc values", f.fieldType().docValuesType(), equalTo(DocValuesType.NONE));
        }
    }

    /**
     * The stored {@code _id} holds the compound {@code id#slice}, so generic stored-field readers must decode it
     * through the field type to expose the plain id.
     */
    public void testStoredIdDecodesToPlainIdNotCompound() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder().put(IndexSettings.SLICE_ENABLED.getKey(), true).build();
        MapperService mapperService = createMapperService(settings, mapping(b -> {}));

        String id = randomAlphaOfLengthBetween(1, 16);
        ParsedDocument doc = mapperService.documentMapper().parse(source(id, b -> {}, "slice-1"));
        BytesRef stored = null;
        for (IndexableField f : doc.rootDoc().getFields(IdFieldMapper.NAME)) {
            if (f.fieldType().stored()) {
                stored = f.binaryValue();
            }
        }
        assertNotNull("document mode stores the compound _id", stored);
        byte[] storedBytes = Arrays.copyOfRange(stored.bytes, stored.offset, stored.offset + stored.length);

        // The raw stored bytes hold the compound, which must never reach a user.
        assertThat(Uid.decodeId(storedBytes), equalTo(id + "#slice-1"));
        assertThat(IdFieldMapper.decodeStoredId(mapperService.fieldType(IdFieldMapper.NAME), storedBytes), equalTo(id));
    }

    public void testColumnarModeStoresCompoundIdInBinaryDocValues() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder()
            .put(IndexSettings.SLICE_ENABLED.getKey(), true)
            .put(IndexSettings.USE_COLUMNAR_ID_BY_DEFAULT.getKey(), true)
            .build();
        MapperService mapperService = createMapperService(settings, mapping(b -> {}));
        assertTrue("columnar default should make the slice _id mapper columnar", sliceIdMapper(mapperService).isColumnarMode());

        String id = randomAlphaOfLengthBetween(1, 16);
        ParsedDocument doc = mapperService.documentMapper().parse(source(id, b -> {}, "slice-1"));
        List<IndexableField> idFields = doc.rootDoc().getFields(IdFieldMapper.NAME);
        // The same two indexed terms (search + compound) but the compound id is in binary doc values, not a stored field.
        assertThat(idFields, hasItem(indexedTerm(SliceIdFieldMapper.searchTerm(id))));
        assertThat(idFields, hasItem(indexedTerm(SliceIdFieldMapper.encodeCompoundId(id, "slice-1"))));
        IndexableField docValues = null;
        for (IndexableField f : idFields) {
            assertFalse("columnar mode must not store _id", f.fieldType().stored());
            if (f.fieldType().docValuesType() == DocValuesType.BINARY) {
                docValues = f;
            }
        }
        assertNotNull("columnar mode must keep the compound id in binary doc values", docValues);
        assertThat(docValues.binaryValue(), equalTo(SliceIdFieldMapper.encodeCompoundId(id, "slice-1")));
    }

    /**
     * Nested children ride in the same Lucene {@code updateDocuments} batch as their root, which requires a consistent
     * field schema, so every non-root document carries the root's compound {@code _id} doc values.
     */
    public void testColumnarModeAddsCompoundIdToNestedDocuments() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder()
            .put(IndexSettings.SLICE_ENABLED.getKey(), true)
            .put(IndexSettings.USE_COLUMNAR_ID_BY_DEFAULT.getKey(), true)
            .build();
        MapperService mapperService = createMapperService(
            settings,
            mapping(b -> b.startObject("nested").field("type", "nested").endObject())
        );

        String id = randomAlphaOfLengthBetween(1, 16);
        ParsedDocument doc = mapperService.documentMapper().parse(source(id, b -> {
            b.startArray("nested");
            b.startObject().field("field", "a").endObject();
            b.startObject().field("field", "b").endObject();
            b.endArray();
        }, "slice-1"));

        BytesRef compound = SliceIdFieldMapper.encodeCompoundId(id, "slice-1");
        assertThat("expected nested documents in addition to the root", doc.docs().size(), greaterThan(1));
        for (LuceneDocument nested : doc.docs()) {
            if (nested == doc.rootDoc()) {
                continue;
            }
            IndexableField docValues = null;
            for (IndexableField f : nested.getFields(IdFieldMapper.NAME)) {
                if (f.fieldType().docValuesType() == DocValuesType.BINARY) {
                    docValues = f;
                }
            }
            assertNotNull("nested documents must carry the _id doc values", docValues);
            assertThat(docValues.binaryValue(), equalTo(compound));
        }
    }

    /**
     * Nested children copy the root's {@code _id} so that a soft-delete by uid removes the whole block, so they must
     * carry the compound identity term.
     */
    public void testNestedDocumentsCarryCompoundIdentityTerm() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder().put(IndexSettings.SLICE_ENABLED.getKey(), true).build();
        MapperService mapperService = createMapperService(
            settings,
            mapping(b -> b.startObject("nested").field("type", "nested").endObject())
        );

        String id = randomAlphaOfLengthBetween(1, 16);
        ParsedDocument doc = mapperService.documentMapper().parse(source(id, b -> {
            b.startArray("nested");
            b.startObject().field("field", "a").endObject();
            b.startObject().field("field", "b").endObject();
            b.endArray();
        }, "slice-1"));

        assertThat("expected nested documents in addition to the root", doc.docs().size(), greaterThan(1));
        BytesRef compound = SliceIdFieldMapper.encodeCompoundId(id, "slice-1");
        for (LuceneDocument nested : doc.docs()) {
            if (nested == doc.rootDoc()) {
                continue;
            }
            assertThat(nested.getFields(IdFieldMapper.NAME), hasItem(indexedTerm(compound)));
        }
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

    public void testCompoundIdRoundTrip() {
        final int iters = 10000;
        for (int iter = 0; iter < iters; ++iter) {
            final String slice = randomSlice();
            final String id = randomId();
            BytesRef compound = SliceIdFieldMapper.encodeCompoundId(id, slice);
            BytesRef search = SliceIdFieldMapper.searchTerm(id);
            // The plain id and the slice round-trip from the compound term...
            assertEquals(id, SliceIdFieldMapper.decodeCompoundId(compound));
            assertEquals(slice, SliceIdFieldMapper.sliceFromCompoundId(compound));
            // ...and the plain id round-trips from the search term (empty-slice member of the same format).
            assertEquals(id, SliceIdFieldMapper.decodeCompoundId(search));
            assertEquals("", SliceIdFieldMapper.sliceFromCompoundId(search));
            // Both terms are standard encodeId round-trips: decodeId(encodeId(id + "#" + slice)) == "id#slice"
            assertEquals(id + "#" + slice, Uid.decodeId(compound.bytes, compound.offset, compound.length));
            assertEquals(id + "#", Uid.decodeId(search.bytes, search.offset, search.length));
        }
    }

    public void testSearchAndCompoundTermSpacesAreDisjoint() {
        for (int iter = 0; iter < 5000; ++iter) {
            String id1 = randomId();
            String id2 = randomId();
            String slice = randomSlice();
            assertNotEquals(SliceIdFieldMapper.searchTerm(id1), SliceIdFieldMapper.encodeCompoundId(id2, slice));
        }
    }

    public void testCompoundSplitsOnLastHashDelimiterForAnyId() {
        // ids may contain '#'; lastIndexOf('#') always splits at the correct boundary since slice values
        // cannot contain '#' (validated by SliceIndexing.VALID_SLICE_VALUE_PATTERN).
        for (String id : new String[] { "a#b", "a#b#c", "with space", "0", "00" }) {
            BytesRef compound = SliceIdFieldMapper.encodeCompoundId(id, "the-slice");
            assertEquals(id, SliceIdFieldMapper.decodeCompoundId(compound));
            assertEquals("the-slice", SliceIdFieldMapper.sliceFromCompoundId(compound));
        }
    }

    public void testSameIdDifferentSlicesAreDistinctCompoundTerms() {
        final String id = randomId();
        BytesRef a = SliceIdFieldMapper.encodeCompoundId(id, "slice-a");
        BytesRef b = SliceIdFieldMapper.encodeCompoundId(id, "slice-b");
        assertNotEquals(a, b);
        assertEquals(id, SliceIdFieldMapper.decodeCompoundId(a));
        assertEquals(id, SliceIdFieldMapper.decodeCompoundId(b));
    }

    public void testCompoundIdRejectsEmptySlice() {
        final String id = randomId();
        // Slices of any length round-trip correctly (no byte-length limit in the #-delimited encoding).
        final String longSlice = randomAlphaOfLength(randomIntBetween(1, 200));
        BytesRef encoded = SliceIdFieldMapper.encodeCompoundId(id, longSlice);
        assertEquals(longSlice, SliceIdFieldMapper.sliceFromCompoundId(encoded));
        assertEquals(id, SliceIdFieldMapper.decodeCompoundId(encoded));
        // An empty slice encodes to the same bytes as the search term (encodeId(id + "#")), so it is rejected.
        expectThrows(IllegalArgumentException.class, () -> SliceIdFieldMapper.encodeCompoundId(id, ""));
    }

    /** The characters {@link org.elasticsearch.index.SliceIndexing#validateUserSliceValue} accepts: {@code [a-zA-Z0-9._:-]}. */
    private static final String SLICE_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:-";

    private static String randomSlice() {
        // Span the full allowed charset; all allowed characters are single-byte ASCII.
        final int length = randomIntBetween(1, 64);
        final StringBuilder slice = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            slice.append(SLICE_CHARS.charAt(randomInt(SLICE_CHARS.length() - 1)));
        }
        return slice.toString();
    }

    private static String randomId() {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> Long.toString(randomNonNegativeLong()); // numeric encoding
            case 1 -> Base64.getUrlEncoder().withoutPadding().encodeToString(randomByteArrayOfLength(randomIntBetween(1, 12))); // base64
            case 2 -> randomAlphaOfLengthBetween(1, 16); // utf8 encoding
            default -> throw new AssertionError("unreachable");
        };
    }
}
