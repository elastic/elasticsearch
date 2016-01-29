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

import com.carrotsearch.hppc.ObjectArrayList;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParsedDocument;

import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
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


        ObjectArrayList<byte[]> bytesList1 = new ObjectArrayList<>(2);
        bytesList1.add(randomBytes());
        bytesList1.add(randomBytes());
        XContentBuilder doc = XContentFactory.jsonBuilder().startObject().startArray("field").value(bytesList1.get(0)).value(bytesList1.get(1)).endArray().endObject();
        ParsedDocument d = mapper.parse("test", "test", "1", doc.bytes());
        writer.addDocument(d.rootDoc());

        byte[] bytes1 = randomBytes();
        doc = XContentFactory.jsonBuilder().startObject().field("field", bytes1).endObject();
        d = mapper.parse("test", "test", "2", doc.bytes());
        writer.addDocument(d.rootDoc());

        doc = XContentFactory.jsonBuilder().startObject().endObject();
        d = mapper.parse("test", "test", "3", doc.bytes());
        writer.addDocument(d.rootDoc());

        // test remove duplicate value
        ObjectArrayList<byte[]> bytesList2 = new ObjectArrayList<>(2);
        bytesList2.add(randomBytes());
        bytesList2.add(randomBytes());
        doc = XContentFactory.jsonBuilder().startObject().startArray("field").value(bytesList2.get(0)).value(bytesList2.get(1)).value(bytesList2.get(0)).endArray().endObject();
        d = mapper.parse("test", "test", "4", doc.bytes());
        writer.addDocument(d.rootDoc());

        LeafReaderContext reader = refreshReader();
        IndexFieldData<?> indexFieldData = getForField("field");
        AtomicFieldData fieldData = indexFieldData.load(reader);

        SortedBinaryDocValues bytesValues = fieldData.getBytesValues();

        CollectionUtils.sortAndDedup(bytesList1);
        bytesValues.setDocument(0);
        assertThat(bytesValues.count(), equalTo(2));
        assertThat(bytesValues.valueAt(0), equalTo(new BytesRef(bytesList1.get(0))));
        assertThat(bytesValues.valueAt(1), equalTo(new BytesRef(bytesList1.get(1))));

        bytesValues.setDocument(1);
        assertThat(bytesValues.count(), equalTo(1));
        assertThat(bytesValues.valueAt(0), equalTo(new BytesRef(bytes1)));

        bytesValues.setDocument(2);
        assertThat(bytesValues.count(), equalTo(0));

        CollectionUtils.sortAndDedup(bytesList2);
        bytesValues.setDocument(3);
        assertThat(bytesValues.count(), equalTo(2));
        assertThat(bytesValues.valueAt(0), equalTo(new BytesRef(bytesList2.get(0))));
        assertThat(bytesValues.valueAt(1), equalTo(new BytesRef(bytesList2.get(1))));
    }

    private byte[] randomBytes() {
        int size = randomIntBetween(10, 1000);
        byte[] bytes = new byte[size];
        getRandom().nextBytes(bytes);
        return bytes;
    }

    @Override
    protected FieldDataType getFieldDataType() {
        return new FieldDataType("binary", Settings.builder().put("format", "doc_values"));
    }
}
