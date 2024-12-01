/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class OffsetSourceFieldMapperTests extends MapperTestCase {
    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new InferencePlugin(Settings.EMPTY));
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "offset_source");
    }

    @Override
    protected Object getSampleValueForDocument() {
        return getSampleObjectForDocument();
    }

    /**
     * Returns a sample object for the field or exception if the field does not support parsing objects
     */
    protected Object getSampleObjectForDocument() {
        return Map.of("field", "foo", "start", 0, "end", 128);
    }

    @Override
    protected void assertExistsQuery(MappedFieldType fieldType, Query query, LuceneDocument fields) {
        assertThat(query, instanceOf(PrefixQuery.class));
        PrefixQuery termQuery = (PrefixQuery) query;
        assertEquals("_offset_source", termQuery.getField());
        assertEquals(new Term("_offset_source", "field"), termQuery.getPrefix());
        assertNotNull(fields.getField("_offset_source"));
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {}

    @Override
    protected void assertSearchable(MappedFieldType fieldType) {
        assertFalse(fieldType.isSearchable());
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    @Override
    protected boolean supportsEmptyInputArray() {
        return false;
    }

    @Override
    protected boolean supportsCopyTo() {
        return false;
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return false;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        return new SyntheticSourceSupport() {
            @Override
            public SyntheticSourceExample example(int maxValues) {
                return new SyntheticSourceExample(getSampleValueForDocument(), getSampleValueForDocument(), null, b -> minimalMapping(b));
            }

            @Override
            public List<SyntheticSourceInvalidExample> invalidExample() {
                return List.of();
            }
        };
    }

    @Override
    public void testSyntheticSourceKeepArrays() {
        // This mapper doesn't support multiple values (array of objects).
    }

    public void testDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        assertEquals(Strings.toString(fieldMapping(this::minimalMapping)), mapper.mappingSource().toString());

        ParsedDocument doc1 = mapper.parse(
            source(b -> b.startObject("field").field("field", "foo").field("start", 0).field("end", 128).endObject())
        );
        List<IndexableField> fields = doc1.rootDoc().getFields("_offset_source");
        assertEquals(1, fields.size());
        assertThat(fields.get(0), instanceOf(OffsetSourceField.class));
        OffsetSourceField offsetField1 = (OffsetSourceField) fields.get(0);

        ParsedDocument doc2 = mapper.parse(
            source(b -> b.startObject("field").field("field", "bar").field("start", 128).field("end", 512).endObject())
        );
        OffsetSourceField offsetField2 = (OffsetSourceField) doc2.rootDoc().getFields("_offset_source").get(0);

        assertTokenStream(offsetField1.tokenStream(null, null), "field.foo", 0, 128);
        assertTokenStream(offsetField2.tokenStream(null, null), "field.bar", 128, 512);
    }

    private void assertTokenStream(TokenStream tk, String expectedTerm, int expectedStartOffset, int expectedEndOffset) throws IOException {
        CharTermAttribute termAttribute = tk.addAttribute(CharTermAttribute.class);
        OffsetAttribute offsetAttribute = tk.addAttribute(OffsetAttribute.class);
        tk.reset();
        assertTrue(tk.incrementToken());
        assertThat(new String(termAttribute.buffer(), 0, termAttribute.length()), equalTo(expectedTerm));
        assertThat(offsetAttribute.startOffset(), equalTo(expectedStartOffset));
        assertThat(offsetAttribute.endOffset(), equalTo(expectedEndOffset));
        assertFalse(tk.incrementToken());
    }

    public void testRejectMultiValuedFields() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> { b.startObject("field").field("type", "offset_source").endObject(); }));

        DocumentParsingException exc = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> {
            b.startArray("field");
            {
                b.startObject().field("field", "bar1").field("start", 128).field("end", 512).endObject();
                b.startObject().field("field", "bar2").field("start", 128).field("end", 512).endObject();
            }
            b.endArray();
        })));
        assertEquals(
            "[offset_source] fields do not support indexing multiple values for the same field [field] in the same document",
            exc.getCause().getMessage()
        );
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("Test implemented in a follow up", true);
        return null;
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }
}
