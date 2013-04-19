/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.unit.index.fielddata;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.plain.ByteArrayAtomicFieldData;
import org.elasticsearch.index.fielddata.plain.ShortArrayAtomicFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 */
public class ShortFieldDataTests extends IntFieldDataTests {

    @Override
    protected FieldDataType getFieldDataType() {
        return new FieldDataType("short");
    }

    @Test
    public void testOptimizeTypeByte() throws Exception {
        Document d = new Document();
        d.add(new StringField("_id", "1", Field.Store.NO));
        d.add(new LongField("value", Byte.MAX_VALUE, Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "2", Field.Store.NO));
        d.add(new LongField("value", Byte.MIN_VALUE, Field.Store.NO));
        writer.addDocument(d);

        IndexNumericFieldData indexFieldData = ifdService.getForField(new FieldMapper.Names("value"), new FieldDataType("long"));
        AtomicNumericFieldData fieldData = indexFieldData.load(refreshReader());
        assertThat(fieldData, instanceOf(ByteArrayAtomicFieldData.class));
        assertThat(fieldData.getLongValues().getValue(0), equalTo((long) Byte.MAX_VALUE));
        assertThat(fieldData.getLongValues().getValue(1), equalTo((long) Byte.MIN_VALUE));
    }

    @Test
    public void testOptimizeTypeShort() throws Exception {
        Document d = new Document();
        d.add(new StringField("_id", "1", Field.Store.NO));
        d.add(new LongField("value", Short.MAX_VALUE, Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "2", Field.Store.NO));
        d.add(new LongField("value", Short.MIN_VALUE, Field.Store.NO));
        writer.addDocument(d);

        IndexNumericFieldData indexFieldData = ifdService.getForField(new FieldMapper.Names("value"), new FieldDataType("long"));
        AtomicNumericFieldData fieldData = indexFieldData.load(refreshReader());
        assertThat(fieldData, instanceOf(ShortArrayAtomicFieldData.class));
        assertThat(fieldData.getLongValues().getValue(0), equalTo((long) Short.MAX_VALUE));
        assertThat(fieldData.getLongValues().getValue(1), equalTo((long) Short.MIN_VALUE));
    }

}
