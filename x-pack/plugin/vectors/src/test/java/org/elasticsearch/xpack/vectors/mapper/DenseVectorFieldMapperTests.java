/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.vectors.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.vectors.Vectors;

import java.io.IOException;
import java.util.Collection;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class DenseVectorFieldMapperTests extends ESSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(Vectors.class, XPackPlugin.class);
    }

    public void testMappingExceedDimsLimit() throws IOException {
        IndexService indexService = createIndex("test-index");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("my-dense-vector").field("type", "dense_vector").field("dims", DenseVectorFieldMapper.MAX_DIMS_COUNT + 1)
            .endObject()
            .endObject()
            .endObject()
            .endObject());
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> parser.parse("_doc", new CompressedXContent(mapping)));
        assertEquals(e.getMessage(), "The number of dimensions for field [my-dense-vector] should be in the range [1, 1024]");
    }

    public void testDefaults() throws Exception {
        IndexService indexService = createIndex("test-index");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("my-dense-vector").field("type", "dense_vector").field("dims", 3)
            .endObject()
            .endObject()
            .endObject()
            .endObject());
        DocumentMapper mapper = parser.parse("_doc", new CompressedXContent(mapping));

        float[] validVector = {-12.1f, 100.7f, -4};
        ParsedDocument doc1 = mapper.parse(new SourceToParse("test-index", "_doc", "1", BytesReference
            .bytes(XContentFactory.jsonBuilder()
                .startObject()
                    .startArray("my-dense-vector").value(validVector[0]).value(validVector[1]).value(validVector[2]).endArray()
                .endObject()),
            XContentType.JSON));
        IndexableField[] fields = doc1.rootDoc().getFields("my-dense-vector");
        assertEquals(1, fields.length);
        assertThat(fields[0], instanceOf(BinaryDocValuesField.class));
        // assert that after decoding the indexed value is equal to expected
        BytesRef vectorBR = fields[0].binaryValue();
        float[] decodedValues = VectorEncoderDecoder.decodeDenseVector(vectorBR);
        assertArrayEquals(
            "Decoded dense vector values is not equal to the indexed one.",
            validVector,
            decodedValues,
            0.001f
        );
    }

    public void testDocumentsWithIncorrectDims() throws Exception {
        IndexService indexService = createIndex("test-index");
        int dims = 3;
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("my-dense-vector").field("type", "dense_vector").field("dims", dims)
            .endObject()
            .endObject()
            .endObject()
            .endObject());
        DocumentMapper mapper = parser.parse("_doc", new CompressedXContent(mapping));

        // test that error is thrown when a document has number of dims more than defined in the mapping
        float[] invalidVector = new float[dims + 1];
        BytesReference invalidDoc = BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .array("my-dense-vector", invalidVector)
            .endObject());
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper.parse(
            new SourceToParse("test-index", "_doc", "1", invalidDoc, XContentType.JSON)));
        assertThat(e.getCause().getMessage(), containsString("has exceeded the number of dimensions [3] defined in mapping"));

        // test that error is thrown when a document has number of dims less than defined in the mapping
        float[] invalidVector2 = new float[dims - 1];
        BytesReference invalidDoc2 = BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .array("my-dense-vector", invalidVector2)
            .endObject());
        MapperParsingException e2 = expectThrows(MapperParsingException.class, () -> mapper.parse(
            new SourceToParse("test-index", "_doc", "2", invalidDoc2, XContentType.JSON)));
        assertThat(e2.getCause().getMessage(), containsString("has number of dimensions [2] less than defined in the mapping [3]"));
    }
}
