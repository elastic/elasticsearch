/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper.NEW_SPARSE_VECTOR_INDEX_VERSION;
import static org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper.PREVIOUS_SPARSE_VECTOR_INDEX_VERSION;
import static org.hamcrest.Matchers.containsString;

public class SparseVectorFieldMapperTests extends MapperTestCase {

    @Override
    protected Object getSampleValueForDocument() {
        return Map.of("ten", 10, "twenty", 20);
    }

    @Override
    protected Object getSampleObjectForDocument() {
        return getSampleValueForDocument();
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "sparse_vector");
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {}

    @Override
    protected boolean supportsMeta() {
        return false;
    }

    private static int getFrequency(TokenStream tk) throws IOException {
        TermFrequencyAttribute freqAttribute = tk.addAttribute(TermFrequencyAttribute.class);
        tk.reset();
        assertTrue(tk.incrementToken());
        int freq = freqAttribute.getTermFrequency();
        assertFalse(tk.incrementToken());
        return freq;
    }

    public void testDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        assertEquals(Strings.toString(fieldMapping(this::minimalMapping)), mapper.mappingSource().toString());

        ParsedDocument doc1 = mapper.parse(source(this::writeField));

        List<IndexableField> fields = doc1.rootDoc().getFields("field");
        assertEquals(2, fields.size());
        assertThat(fields.get(0), Matchers.instanceOf(FeatureField.class));
        FeatureField featureField1 = null;
        FeatureField featureField2 = null;
        for (IndexableField field : fields) {
            if (field.stringValue().equals("ten")) {
                featureField1 = (FeatureField) field;
            } else if (field.stringValue().equals("twenty")) {
                featureField2 = (FeatureField) field;
            } else {
                throw new UnsupportedOperationException();
            }
        }

        int freq1 = getFrequency(featureField1.tokenStream(null, null));
        int freq2 = getFrequency(featureField2.tokenStream(null, null));
        assertTrue(freq1 < freq2);
    }

    public void testDotInFieldName() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        DocumentParsingException ex = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.field("field", Map.of("politi.cs", 10, "sports", 20))))
        );
        assertThat(ex.getCause().getMessage(), containsString("do not support dots in feature names"));
        assertThat(ex.getCause().getMessage(), containsString("politi.cs"));
    }

    public void testRejectMultiValuedFields() throws MapperParsingException, IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("field").field("type", "sparse_vector").endObject();
            b.startObject("foo").startObject("properties");
            {
                b.startObject("field").field("type", "sparse_vector").endObject();
            }
            b.endObject().endObject();
        }));

        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject("field").field("foo", Arrays.asList(10, 20)).endObject()))
        );
        assertEquals(
            "[sparse_vector] fields take hashes that map a feature to a strictly positive float, but got unexpected token " + "START_ARRAY",
            e.getCause().getMessage()
        );

        e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> {
            b.startArray("foo");
            {
                b.startObject().startObject("field").field("bar", 10).endObject().endObject();
                b.startObject().startObject("field").field("bar", 20).endObject().endObject();
            }
            b.endArray();
        })));
        assertEquals(
            "[sparse_vector] fields do not support indexing multiple values for the same feature [foo.field.bar] in " + "the same document",
            e.getCause().getMessage()
        );
    }

    public void testCannotBeUsedInMultiFields() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "keyword");
            b.startObject("fields");
            b.startObject("feature");
            b.field("type", "sparse_vector");
            b.endObject();
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("Field [feature] of type [sparse_vector] can't be used in multifields"));
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("Test implemented in a follow up", true);
        return null;
    }

    @Override
    protected boolean allowsNullValues() {
        return false;       // TODO should this allow null values?
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean syntheticSource) {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected String[] getParseMinimalWarnings(IndexVersion indexVersion) {
        String[] additionalWarnings = null;
        if (indexVersion.before(PREVIOUS_SPARSE_VECTOR_INDEX_VERSION)) {
            additionalWarnings = new String[] { SparseVectorFieldMapper.ERROR_MESSAGE_7X };
        }
        return Strings.concatStringArrays(super.getParseMinimalWarnings(indexVersion), additionalWarnings);
    }

    @Override
    protected IndexVersion boostNotAllowedIndexVersion() {
        return NEW_SPARSE_VECTOR_INDEX_VERSION;
    }

    public void testSparseVectorWith7xIndex() throws Exception {
        IndexVersion version = IndexVersionUtils.randomPreviousCompatibleVersion(random(), PREVIOUS_SPARSE_VECTOR_INDEX_VERSION);

        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("my-vector")
            .field("type", "sparse_vector")
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        DocumentMapper mapper = createDocumentMapper(version, builder);
        assertWarnings(SparseVectorFieldMapper.ERROR_MESSAGE_7X);

        // Check that new vectors cannot be indexed.
        int[] indexedDims = { 65535, 50, 2 };
        float[] indexedValues = { 0.5f, 1800f, -34567.11f };
        BytesReference source = BytesReference.bytes(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("my-vector")
                .field(Integer.toString(indexedDims[0]), indexedValues[0])
                .field(Integer.toString(indexedDims[1]), indexedValues[1])
                .field(Integer.toString(indexedDims[2]), indexedValues[2])
                .endObject()
                .endObject()
        );

        DocumentParsingException indexException = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(new SourceToParse("id", source, XContentType.JSON))
        );
        assertThat(indexException.getCause().getMessage(), containsString(SparseVectorFieldMapper.ERROR_MESSAGE_7X));
    }

    public void testSparseVectorUnsupportedIndex() throws Exception {
        IndexVersion version = IndexVersionUtils.randomVersionBetween(
            random(),
            PREVIOUS_SPARSE_VECTOR_INDEX_VERSION,
            IndexVersions.FIRST_DETACHED_INDEX_VERSION
        );
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(version, fieldMapping(b -> {
            b.field("type", "sparse_vector");
        })));
        assertThat(e.getMessage(), containsString(SparseVectorFieldMapper.ERROR_MESSAGE_8X));
    }
}
