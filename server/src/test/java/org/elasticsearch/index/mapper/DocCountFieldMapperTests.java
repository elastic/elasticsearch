/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DocCountFieldMapperTests extends MapperServiceTestCase {

    private static final String CONTENT_TYPE = DocCountFieldMapper.CONTENT_TYPE;
    private static final String DOC_COUNT_FIELD = DocCountFieldMapper.NAME;

    public void testParseValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.field("foo", 500).field(CONTENT_TYPE, 100)));

        IndexableField field = doc.rootDoc().getField(DOC_COUNT_FIELD);
        assertEquals(DOC_COUNT_FIELD, field.stringValue());
        assertEquals(1, doc.rootDoc().getFields(DOC_COUNT_FIELD).length);
    }

    public void testInvalidDocument_NegativeDocCount() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field(CONTENT_TYPE, -100))));
        assertThat(e.getCause().getMessage(), containsString("Field [_doc_count] must be a positive integer"));
    }

    public void testInvalidDocument_ZeroDocCount() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field(CONTENT_TYPE, 0))));
        assertThat(e.getCause().getMessage(), containsString("Field [_doc_count] must be a positive integer"));
    }

    public void testInvalidDocument_NonNumericDocCount() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field(CONTENT_TYPE, "foo"))));
        assertThat(
            e.getCause().getMessage(),
            containsString("Failed to parse object: expecting token of type [VALUE_NUMBER] but found [VALUE_STRING]")
        );
    }

    public void testInvalidDocument_FractionalDocCount() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field(CONTENT_TYPE, 100.23))));
        assertThat(e.getCause().getMessage(), containsString("100.23 cannot be converted to Integer without data loss"));
    }

    public void testInvalidDocument_ArrayDocCount() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.array(CONTENT_TYPE, 10, 20, 30))));
        assertThat(e.getCause().getMessage(), containsString("Arrays are not allowed for field [_doc_count]."));
    }

    public void testSyntheticSource() throws IOException {
        DocumentMapper mapper = createDocumentMapper(syntheticSourceMapping(b -> {}));
        assertThat(syntheticSource(mapper, b -> b.field(CONTENT_TYPE, 10)), equalTo("{\"_doc_count\":10}"));
    }

    public void testSyntheticSourceMany() throws IOException {
        MapperService mapper = createMapperService(syntheticSourceMapping(b -> {}));
        List<Integer> counts = randomList(2, 10000, () -> between(1, Integer.MAX_VALUE));
        withLuceneIndex(mapper, iw -> {
            for (int c : counts) {
                iw.addDocument(mapper.documentMapper().parse(source(b -> b.field(CONTENT_TYPE, c))).rootDoc());
            }
        }, reader -> {
            int i = 0;
            SourceLoader loader = mapper.mappingLookup().newSourceLoader();
            assertTrue(loader.requiredStoredFields().isEmpty());
            for (LeafReaderContext leaf : reader.leaves()) {
                int[] docIds = IntStream.range(0, leaf.reader().maxDoc()).toArray();
                SourceLoader.Leaf sourceLoaderLeaf = loader.leaf(leaf.reader(), docIds);
                LeafStoredFieldLoader storedFieldLoader = StoredFieldLoader.empty().getLoader(leaf, docIds);
                for (int docId : docIds) {
                    assertThat(
                        "doc " + docId,
                        sourceLoaderLeaf.source(storedFieldLoader, docId).utf8ToString(),
                        equalTo("{\"_doc_count\":" + counts.get(i++) + "}")
                    );
                }
            }
        });
    }
}
