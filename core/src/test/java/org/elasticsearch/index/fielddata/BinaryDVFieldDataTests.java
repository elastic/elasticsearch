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

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;

import java.util.ArrayList;
import java.util.List;

public class BinaryDVFieldDataTests extends AbstractFieldDataTestCase {
    @Override
    protected boolean hasDocValues() {
        return true;
    }

    public void testDocValue() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("test")
                .startObject("properties")
                .startObject("field")
                .field("type", "binary")
                .field("doc_values", true)
                .endObject()
                .endObject()
                .endObject().endObject().string();

        final DocumentMapper mapper = mapperService.documentMapperParser().parse("test", new CompressedXContent(mapping));


        List<BytesRef> bytesList1 = new ArrayList<>(2);
        bytesList1.add(randomBytes());
        bytesList1.add(randomBytes());
        XContentBuilder doc = XContentFactory.jsonBuilder().startObject();
        {
            doc.startArray("field");
            doc.value(bytesList1.get(0));
            doc.value(bytesList1.get(1));
            doc.endArray();
        }
        doc.endObject();
        ParsedDocument d = mapper.parse(SourceToParse.source("test", "test", "1", doc.bytes(), XContentType.JSON));
        writer.addDocument(d.rootDoc());

        BytesRef bytes1 = randomBytes();
        doc = XContentFactory.jsonBuilder().startObject().field("field", bytes1).endObject();
        d = mapper.parse(SourceToParse.source("test", "test", "2", doc.bytes(), XContentType.JSON));
        writer.addDocument(d.rootDoc());

        doc = XContentFactory.jsonBuilder().startObject().endObject();
        d = mapper.parse(SourceToParse.source("test", "test", "3", doc.bytes(), XContentType.JSON));
        writer.addDocument(d.rootDoc());

        // test remove duplicate value
        List<BytesRef> bytesList2 = new ArrayList<>(2);
        bytesList2.add(randomBytes());
        bytesList2.add(randomBytes());
        doc = XContentFactory.jsonBuilder().startObject();
        {
            doc.startArray("field");
            doc.value(bytesList2.get(0));
            doc.value(bytesList2.get(1));
            doc.value(bytesList2.get(0));
            doc.endArray();
        }
        doc.endObject();
        d = mapper.parse(SourceToParse.source("test", "test", "4", doc.bytes(), XContentType.JSON));
        writer.addDocument(d.rootDoc());

        IndexFieldData<?> indexFieldData = getForField("field");
        List<LeafReaderContext> readers = refreshReader();
        assertEquals(1, readers.size());
        LeafReaderContext reader = readers.get(0);

        bytesList1.sort(null);
        bytesList2.sort(null);

        // Test SortedBinaryDocValues's decoding:
        AtomicFieldData fieldData = indexFieldData.load(reader);
        SortedBinaryDocValues bytesValues = fieldData.getBytesValues();

        assertTrue(bytesValues.advanceExact(0));
        assertEquals(2, bytesValues.docValueCount());
        assertEquals(bytesList1.get(0), bytesValues.nextValue());
        assertEquals(bytesList1.get(1), bytesValues.nextValue());

        assertTrue(bytesValues.advanceExact(1));
        assertEquals(1, bytesValues.docValueCount());
        assertEquals(bytes1, bytesValues.nextValue());

        assertFalse(bytesValues.advanceExact(2));

        assertTrue(bytesValues.advanceExact(3));
        assertEquals(2, bytesValues.docValueCount());
        assertEquals(bytesList2.get(0), bytesValues.nextValue());
        assertEquals(bytesList2.get(1), bytesValues.nextValue());

        // Test whether ScriptDocValues.BytesRefs makes a deepcopy
        fieldData = indexFieldData.load(reader);
        ScriptDocValues<?> scriptValues = fieldData.getScriptValues();
        scriptValues.setNextDocId(0);
        assertEquals(2, scriptValues.size());
        assertEquals(bytesList1.get(0), scriptValues.get(0));
        assertEquals(bytesList1.get(1), scriptValues.get(1));

        scriptValues.setNextDocId(1);
        assertEquals(1, scriptValues.size());
        assertEquals(bytes1, scriptValues.get(0));

        scriptValues.setNextDocId(2);
        assertEquals(0, scriptValues.size());

        scriptValues.setNextDocId(3);
        assertEquals(2, scriptValues.size());
        assertEquals(bytesList2.get(0), scriptValues.get(0));
        assertEquals(bytesList2.get(1), scriptValues.get(1));
    }

    private static BytesRef randomBytes() {
        int size = randomIntBetween(10, 1000);
        byte[] bytes = new byte[size];
        random().nextBytes(bytes);
        return new BytesRef(bytes);
    }

    @Override
    protected String getFieldDataType() {
        return "binary";
    }
}
