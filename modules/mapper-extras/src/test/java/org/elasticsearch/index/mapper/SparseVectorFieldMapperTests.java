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

import java.util.Collection;

public class SparseVectorFieldMapperTests extends ESSingleNodeTestCase {

   @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(MapperExtrasPlugin.class);
    }

    public void testDefaults() throws Exception {
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

        DocumentMapper mapper = parser.parse("_doc", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        int[] indexedDims = {65000, 50, 2};
        float[] indexedValues = {0.5f, 1800f, -34567.11f};
        ParsedDocument doc1 = mapper.parse(SourceToParse.source("test-index", "_doc", "1", BytesReference
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
        int[] expectedDims = {2, 50, 65000}; //the same as indexed but sorted
        float[] expectedValues = {-34567.11f, 1800f, 0.5f}; //the same as indexed but sorted by their dimensions

        // assert that after decoding the indexed value is equal to expected
        BytesRef vectorBR = ((BinaryDocValuesField) fields[0]).binaryValue();
        float[] decodedValues = SparseVectorFieldMapper.decodeVector(vectorBR);
        assertArrayEquals(
            "Decoded sparse vector values are not equal to the indexed ones.",
            expectedValues,
            decodedValues,
            0.001f
        );
        int[] decodedDims = SparseVectorFieldMapper.decodeVectorDims(vectorBR, decodedValues.length);
        assertArrayEquals(
            "Decoded sparse vector dimensions are not equal to the indexed ones.",
            expectedDims,
            decodedDims
        );
    }
}
