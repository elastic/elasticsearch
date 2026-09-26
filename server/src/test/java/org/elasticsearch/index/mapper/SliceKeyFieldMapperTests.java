/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * Slice-enabled indices carry two derived metadata fields per routed document: the hash-prefixed slice key that the index
 * is sorted on, and the bare hash as a skip-indexed numeric column. Both are written on the root and on nested children.
 */
public class SliceKeyFieldMapperTests extends MapperServiceTestCase {

    private Settings sliceEnabledSettings() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        return Settings.builder().put(getIndexSettings()).put(IndexSettings.SLICE_ENABLED.getKey(), true).build();
    }

    private MapperService sliceEnabledMapperService() throws Exception {
        return createMapperService(sliceEnabledSettings(), mapping(b -> {
            b.startObject("n");
            b.field("type", "nested");
            b.startObject("properties");
            b.startObject("k").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
    }

    public void testWritesKeyAndHashForRoutedDocument() throws Exception {
        MapperService mapperService = sliceEnabledMapperService();
        String slice = randomAlphaOfLengthBetween(1, 20);
        ParsedDocument doc = mapperService.documentMapper().parse(source("1", b -> b.field("f", "v"), slice));
        assertSliceFields(doc.rootDoc(), slice);
    }

    public void testNestedChildrenCarrySliceFields() throws Exception {
        MapperService mapperService = sliceEnabledMapperService();
        String slice = randomAlphaOfLengthBetween(1, 20);
        ParsedDocument doc = mapperService.documentMapper().parse(source("1", b -> {
            b.startArray("n");
            b.startObject().field("k", "v1").endObject();
            b.startObject().field("k", "v2").endObject();
            b.endArray();
        }, slice));
        assertThat(doc.docs(), hasSize(3));
        assertSliceFields(doc.rootDoc(), slice);
        // Diversifying-children sliced vector queries search child documents, so they need the key and hash too.
        for (LuceneDocument child : doc.docs().subList(0, doc.docs().size() - 1)) {
            assertSliceFields(child, slice);
        }
    }

    public void testAbsentWhenSliceDisabled() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        assertNull(mapperService.fieldType(SliceKeyFieldMapper.NAME));
        ParsedDocument doc = mapperService.documentMapper().parse(source("1", b -> b.field("f", "v"), "routing_value"));
        assertThat(doc.rootDoc().getFields(SliceKeyFieldMapper.NAME), empty());
        assertThat(doc.rootDoc().getFields(SliceIndexing.SLICE_HASH_FIELD_NAME), empty());
    }

    public void testFieldTypeIsNotSearchable() throws Exception {
        MapperService mapperService = sliceEnabledMapperService();
        MappedFieldType ft = mapperService.fieldType(SliceKeyFieldMapper.NAME);
        assertNotNull(ft);
        assertFalse(ft.isSearchable());
        assertTrue(ft.hasDocValues());
        SearchExecutionContext context = createSearchExecutionContext(mapperService);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ft.termQuery("x", context));
        assertThat(e.getMessage(), equalTo("[" + SliceKeyFieldMapper.NAME + "] is not searchable"));
        IllegalArgumentException f = expectThrows(IllegalArgumentException.class, () -> ft.valueFetcher(context, null));
        assertThat(f.getMessage(), equalTo("[" + SliceKeyFieldMapper.NAME + "] is not fetchable"));
    }

    /** The key and hash are layout internals: a search context must not resolve them by name or by wildcard. */
    public void testInternalFieldsAreHiddenFromSearchContext() throws Exception {
        MapperService mapperService = sliceEnabledMapperService();
        SearchExecutionContext context = createSearchExecutionContext(mapperService);
        for (String name : List.of(SliceKeyFieldMapper.NAME, SliceIndexing.SLICE_HASH_FIELD_NAME)) {
            assertFalse(name, context.isFieldMapped(name));
            assertNull(name, context.getFieldType(name));
            assertThat(name, context.getMatchingFieldNames(name), empty());
            assertThat(name, context.getMatchingFieldNames("*"), not(hasItem(name)));
            assertThat(name, context.getMatchingFieldNames("_slice*"), not(hasItem(name)));
        }
        // _routing stays resolvable by type so slice routing filters keep working.
        assertNotNull(context.getFieldType(RoutingFieldMapper.NAME));
    }

    /** Users may not map the internal field names, as properties or as runtime fields. */
    public void testInternalFieldNamesAreReserved() {
        Settings settings = sliceEnabledSettings();
        for (String name : List.of(SliceKeyFieldMapper.NAME, SliceIndexing.SLICE_HASH_FIELD_NAME)) {
            Exception property = expectThrows(
                Exception.class,
                () -> createMapperService(settings, mapping(b -> b.startObject(name).field("type", "keyword").endObject()))
            );
            assertThat(property.getMessage(), containsString("[" + name + "]"));
            Exception runtime = expectThrows(
                Exception.class,
                () -> createMapperService(settings, runtimeMapping(b -> b.startObject(name).field("type", "keyword").endObject()))
            );
            assertThat(runtime.getMessage(), containsString("[" + name + "] is a reserved field name"));
        }
        Exception hashProperty = expectThrows(
            Exception.class,
            () -> createMapperService(
                settings,
                mapping(b -> b.startObject(SliceIndexing.SLICE_HASH_FIELD_NAME).field("type", "long").endObject())
            )
        );
        assertThat(hashProperty.getMessage(), containsString("[" + SliceIndexing.SLICE_HASH_FIELD_NAME + "] is a reserved field name"));
    }

    private static void assertSliceFields(LuceneDocument doc, String slice) {
        List<IndexableField> keyFields = doc.getFields(SliceKeyFieldMapper.NAME);
        assertThat(keyFields, hasSize(1));
        IndexableField keyField = keyFields.get(0);
        assertThat(keyField.fieldType().docValuesType(), equalTo(DocValuesType.SORTED));
        assertThat(keyField.fieldType().docValuesSkipIndexType(), not(equalTo(DocValuesSkipIndexType.NONE)));
        BytesRef key = keyField.binaryValue();
        assertThat(key, equalTo(SliceIndexing.encodeSliceKey(slice)));
        assertThat(SliceIndexing.sliceFromKey(key), equalTo(slice));

        List<IndexableField> hashFields = doc.getFields(SliceIndexing.SLICE_HASH_FIELD_NAME);
        assertThat(hashFields, hasSize(1));
        IndexableField hashField = hashFields.get(0);
        assertThat(hashField.fieldType().docValuesType(), equalTo(DocValuesType.SORTED_NUMERIC));
        assertThat(hashField.fieldType().docValuesSkipIndexType(), not(equalTo(DocValuesSkipIndexType.NONE)));
        long hash = hashField.numericValue().longValue();
        assertThat(hash, equalTo(SliceIndexing.sliceHash(slice)));
        assertThat(hash, equalTo(SliceIndexing.sliceHashFromKey(key)));
    }
}
