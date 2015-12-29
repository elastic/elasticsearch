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

package org.elasticsearch.index.mapper.binary;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.core.BinaryFieldMapper;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 */
public class BinaryMappingTests extends ESSingleNodeTestCase {

    public void testDefaultMapping() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field")
                .field("type", "binary")
                .endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        FieldMapper fieldMapper = mapper.mappers().smartNameFieldMapper("field");
        assertThat(fieldMapper, instanceOf(BinaryFieldMapper.class));
        assertThat(fieldMapper.fieldType().stored(), equalTo(false));
    }

    public void testStoredValue() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field")
                .field("type", "binary")
                .field("store", "yes")
                .endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        // case 1: a simple binary value
        final byte[] binaryValue1 = new byte[100];
        binaryValue1[56] = 1;

        // case 2: a value that looks compressed: this used to fail in 1.x
        BytesStreamOutput out = new BytesStreamOutput();
        try (StreamOutput compressed = CompressorFactory.defaultCompressor().streamOutput(out)) {
            new BytesArray(binaryValue1).writeTo(compressed);
        }
        final byte[] binaryValue2 = out.bytes().toBytes();
        assertTrue(CompressorFactory.isCompressed(new BytesArray(binaryValue2)));

        for (byte[] value : Arrays.asList(binaryValue1, binaryValue2)) {
            ParsedDocument doc = mapper.parse("test", "type", "id", XContentFactory.jsonBuilder().startObject().field("field", value).endObject().bytes());
            BytesRef indexedValue = doc.rootDoc().getBinaryValue("field");
            assertEquals(new BytesRef(value), indexedValue);
            FieldMapper fieldMapper = mapper.mappers().smartNameFieldMapper("field");
            Object originalValue = fieldMapper.fieldType().valueForSearch(indexedValue);
            assertEquals(new BytesArray(value), originalValue);
        }
    }
}
