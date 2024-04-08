/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.script.field.BinaryDocValuesField;
import org.elasticsearch.script.field.ScriptFieldFactory;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BinaryDVFieldDataTests extends AbstractFieldDataTestCase {
    @Override
    protected boolean hasDocValues() {
        return true;
    }

    public void testDocValue() throws Exception {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("test")
                .startObject("properties")
                .startObject("field")
                .field("type", "binary")
                .field("doc_values", true)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        DocumentMapper mapper = mapperService.merge("test", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

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
        ParsedDocument d = mapper.parse(new SourceToParse("1", BytesReference.bytes(doc), XContentType.JSON));
        writer.addDocument(d.rootDoc());

        BytesRef bytes1 = randomBytes();
        doc = XContentFactory.jsonBuilder().startObject().field("field", bytes1.bytes, bytes1.offset, bytes1.length).endObject();
        d = mapper.parse(new SourceToParse("2", BytesReference.bytes(doc), XContentType.JSON));
        writer.addDocument(d.rootDoc());

        doc = XContentFactory.jsonBuilder().startObject().endObject();
        d = mapper.parse(new SourceToParse("3", BytesReference.bytes(doc), XContentType.JSON));
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
        d = mapper.parse(new SourceToParse("4", BytesReference.bytes(doc), XContentType.JSON));
        writer.addDocument(d.rootDoc());

        IndexFieldData<?> indexFieldData = getForField("field");
        List<LeafReaderContext> readers = refreshReader();
        assertEquals(1, readers.size());
        LeafReaderContext reader = readers.get(0);

        bytesList1.sort(null);
        bytesList2.sort(null);

        // Test SortedBinaryDocValues's decoding:
        LeafFieldData fieldData = indexFieldData.load(reader);
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

        // Test whether BinaryDocValuesField makes a deepcopy
        fieldData = indexFieldData.load(reader);
        ScriptFieldFactory factory = fieldData.getScriptFieldFactory("test");
        BinaryDocValuesField binaryDocValuesField = (BinaryDocValuesField) factory.toScriptField();
        ByteBuffer[][] retValues = new ByteBuffer[4][];
        for (int i = 0; i < 4; i++) {
            binaryDocValuesField.setNextDocId(i);
            retValues[i] = new ByteBuffer[binaryDocValuesField.size()];
            for (int j = 0; j < retValues[i].length; j++) {
                retValues[i][j] = binaryDocValuesField.get(j, null);
            }
        }
        assertEquals(2, retValues[0].length);
        assertArrayEquals(bytesList1.get(0).bytes, retValues[0][0].array());
        assertArrayEquals(bytesList1.get(1).bytes, retValues[0][1].array());

        assertEquals(1, retValues[1].length);
        assertArrayEquals(bytes1.bytes, retValues[1][0].array());

        assertEquals(0, retValues[2].length);

        assertEquals(2, retValues[3].length);
        assertArrayEquals(bytesList2.get(0).bytes, retValues[3][0].array());
        assertArrayEquals(bytesList2.get(1).bytes, retValues[3][1].array());
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
