/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.Collection;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class SparseVectorFieldMapperTests extends ESSingleNodeTestCase {
    private DocumentMapper mapper;

    @Before
    public void setup() throws Exception {
        IndexService indexService =  createIndex("test-index");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("my-sparse-vector").field("type", "sparse_vector")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());
        mapper = parser.parse("_doc", new CompressedXContent(mapping));
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(MapperExtrasPlugin.class);
    }

    public void testDefaults() throws Exception {
        int[] indexedDims = {65535, 50, 2};
        float[] indexedValues = {0.5f, 1800f, -34567.11f};
        ParsedDocument doc1 = mapper.parse(new SourceToParse("test-index", "_doc", "1", BytesReference
            .bytes(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("my-sparse-vector")
                        .field(Integer.toString(indexedDims[0]), indexedValues[0])
                        .field(Integer.toString(indexedDims[1]), indexedValues[1])
                        .field(Integer.toString(indexedDims[2]), indexedValues[2])
                    .endObject()
                .endObject()),
            XContentType.JSON));
        IndexableField[] fields = doc1.rootDoc().getFields("my-sparse-vector");
        assertEquals(1, fields.length);
        assertThat(fields[0], Matchers.instanceOf(BinaryDocValuesField.class));

        // assert that after decoding the indexed values are equal to expected
        int[] expectedDims = {2, 50, 65535}; //the same as indexed but sorted
        float[] expectedValues = {-34567.11f, 1800f, 0.5f}; //the same as indexed but sorted by their dimensions

        // assert that after decoding the indexed dims and values are equal to expected
        BytesRef vectorBR = ((BinaryDocValuesField) fields[0]).binaryValue();
        int[] decodedDims = VectorEncoderDecoder.decodeSparseVectorDims(vectorBR);
        assertArrayEquals(
            "Decoded sparse vector dimensions are not equal to the indexed ones.",
            expectedDims,
            decodedDims
        );
        float[] decodedValues = VectorEncoderDecoder.decodeSparseVector(vectorBR);
        assertArrayEquals(
            "Decoded sparse vector values are not equal to the indexed ones.",
            expectedValues,
            decodedValues,
            0.001f
        );
    }

    public void testErrors() {
        // 1. test for an error on negative dimension
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            mapper.parse(new SourceToParse("test-index", "_doc", "1", BytesReference
            .bytes(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("my-sparse-vector")
                        .field(Integer.toString(-50), 100f)
                    .endObject()
                .endObject()),
            XContentType.JSON));
        });
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(e.getCause().getMessage(), containsString(
            "dimension number must be a non-negative integer value not exceeding [65535], got [-50]"));

        // 2. test for an error on a dimension greater than MAX_DIMS_NUMBER
        e = expectThrows(MapperParsingException.class, () -> {
            mapper.parse(new SourceToParse("test-index", "_doc", "1", BytesReference
            .bytes(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("my-sparse-vector")
                        .field(Integer.toString(70000), 100f)
                    .endObject()
                .endObject()),
            XContentType.JSON));
        });
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(e.getCause().getMessage(), containsString(
            "dimension number must be a non-negative integer value not exceeding [65535], got [70000]"));

        // 3. test for an error on a wrong formatted dimension
        e = expectThrows(MapperParsingException.class, () -> {
            mapper.parse(new SourceToParse("test-index", "_doc", "1", BytesReference
            .bytes(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("my-sparse-vector")
                        .field("WrongDim123", 100f)
                    .endObject()
                .endObject()),
            XContentType.JSON));
        });
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(e.getCause().getMessage(), containsString(
            "dimensions should be integers represented as strings, but got [WrongDim123]"));

         // 4. test for an error on a wrong format for the map of dims to values
        e = expectThrows(MapperParsingException.class, () -> {
            mapper.parse(new SourceToParse("test-index", "_doc", "1", BytesReference
            .bytes(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("my-sparse-vector")
                        .startArray(Integer.toString(10)).value(10f).value(100f).endArray()
                    .endObject()
                .endObject()),
            XContentType.JSON));
        });
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(e.getCause().getMessage(), containsString(
            "takes an object that maps a dimension number to a float, but got unexpected token [START_ARRAY]"));
    }
}
