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
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DocCountFieldMapperTests extends MetadataMapperTestCase {

    private static final String CONTENT_TYPE = DocCountFieldMapper.CONTENT_TYPE;
    private static final String DOC_COUNT_FIELD = DocCountFieldMapper.NAME;

    @Override
    protected String fieldName() {
        return DocCountFieldMapper.NAME;
    }

    @Override
    protected boolean isConfigurable() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {}

    public void testParseValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.field("foo", 500).field(CONTENT_TYPE, 100)));

        IndexableField field = doc.rootDoc().getField(DOC_COUNT_FIELD);
        assertEquals(DOC_COUNT_FIELD, field.stringValue());
        assertEquals(1, doc.rootDoc().getFields(DOC_COUNT_FIELD).size());
    }

    public void testInvalidDocument_NegativeDocCount() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.field(CONTENT_TYPE, -100))));
        assertThat(e.getCause().getMessage(), containsString("Field [_doc_count] must be a positive integer"));
    }

    public void testInvalidDocument_ZeroDocCount() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.field(CONTENT_TYPE, 0))));
        assertThat(e.getCause().getMessage(), containsString("Field [_doc_count] must be a positive integer"));
    }

    public void testInvalidDocument_NonNumericDocCount() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.field(CONTENT_TYPE, "foo"))));
        assertThat(
            e.getCause().getMessage(),
            containsString("Failed to parse object: expecting token of type [VALUE_NUMBER] but found [VALUE_STRING]")
        );
    }

    public void testInvalidDocument_FractionalDocCount() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.field(CONTENT_TYPE, 100.23))));
        assertThat(e.getCause().getMessage(), containsString("100.23 cannot be converted to Integer without data loss"));
    }

    public void testInvalidDocument_ArrayDocCount() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.array(CONTENT_TYPE, 10, 20, 30))));
        assertThat(e.getCause().getMessage(), containsString("Arrays are not allowed for field [_doc_count]."));
    }

    public void testSyntheticSource() throws IOException {
        DocumentMapper mapper = createDocumentMapper(syntheticSourceMapping(b -> {}));
        assertThat(syntheticSource(mapper, b -> b.field(CONTENT_TYPE, 10)), equalTo("{\"_doc_count\":10}"));
    }

    public void testSyntheticSourceMany() throws IOException {
        MapperService mapper = createMapperService(syntheticSourceMapping(b -> b.startObject("doc").field("type", "integer").endObject()));
        List<Integer> counts = randomList(2, 10000, () -> between(1, Integer.MAX_VALUE));
        withLuceneIndex(mapper, iw -> {
            int d = 0;
            for (int c : counts) {
                int doc = d++;
                iw.addDocument(mapper.documentMapper().parse(source(b -> b.field("doc", doc).field(CONTENT_TYPE, c))).rootDoc());
            }
        }, reader -> {
            SourceLoader loader = mapper.mappingLookup().newSourceLoader();
            assertTrue(loader.requiredStoredFields().isEmpty());
            for (LeafReaderContext leaf : reader.leaves()) {
                int[] docIds = IntStream.range(0, leaf.reader().maxDoc()).toArray();
                SourceLoader.Leaf sourceLoaderLeaf = loader.leaf(leaf.reader(), docIds);
                LeafStoredFieldLoader storedFieldLoader = StoredFieldLoader.empty().getLoader(leaf, docIds);
                for (int docId : docIds) {
                    String source = sourceLoaderLeaf.source(storedFieldLoader, docId).internalSourceRef().utf8ToString();
                    int doc = (int) JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, source).map().get("doc");
                    assertThat("doc " + docId, source, equalTo("{\"_doc_count\":" + counts.get(doc) + ",\"doc\":" + doc + "}"));
                }
            }
        });
    }

    public void testSyntheticSourceManyDoNotHave() throws IOException {
        MapperService mapper = createMapperService(syntheticSourceMapping(b -> b.startObject("doc").field("type", "integer").endObject()));
        List<Integer> counts = randomList(2, 10000, () -> randomBoolean() ? null : between(1, Integer.MAX_VALUE));
        withLuceneIndex(mapper, iw -> {
            int d = 0;
            for (Integer c : counts) {
                int doc = d++;
                iw.addDocument(mapper.documentMapper().parse(source(b -> {
                    b.field("doc", doc);
                    if (c != null) {
                        b.field(CONTENT_TYPE, c);
                    }
                })).rootDoc());
            }
        }, reader -> {
            SourceLoader loader = mapper.mappingLookup().newSourceLoader();
            assertTrue(loader.requiredStoredFields().isEmpty());
            for (LeafReaderContext leaf : reader.leaves()) {
                int[] docIds = IntStream.range(0, leaf.reader().maxDoc()).toArray();
                SourceLoader.Leaf sourceLoaderLeaf = loader.leaf(leaf.reader(), docIds);
                LeafStoredFieldLoader storedFieldLoader = StoredFieldLoader.empty().getLoader(leaf, docIds);
                for (int docId : docIds) {
                    String source = sourceLoaderLeaf.source(storedFieldLoader, docId).internalSourceRef().utf8ToString();
                    int doc = (int) JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, source).map().get("doc");
                    String docCountPart = counts.get(doc) == null ? "" : "\"_doc_count\":" + counts.get(doc) + ",";
                    assertThat("doc " + docId, source, equalTo("{" + docCountPart + "\"doc\":" + doc + "}"));
                }
            }
        });
    }
}
