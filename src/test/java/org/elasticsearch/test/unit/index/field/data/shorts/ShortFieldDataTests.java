/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.unit.index.field.data.shorts;

import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.field.data.shorts.ShortFieldData;
import org.testng.annotations.Test;

import java.util.ArrayList;

import static org.elasticsearch.common.lucene.DocumentBuilder.doc;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class ShortFieldDataTests {

    @Test
    public void intFieldDataTests() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        indexWriter.addDocument(doc()
                .add(new NumericField("svalue").setIntValue(4))
                .add(new NumericField("mvalue").setIntValue(104))
                .build());

        indexWriter.addDocument(doc()
                .add(new NumericField("svalue").setIntValue(3))
                .add(new NumericField("mvalue").setIntValue(104))
                .add(new NumericField("mvalue").setIntValue(105))
                .build());

        indexWriter.addDocument(doc()
                .add(new NumericField("svalue").setIntValue(7))
                .build());

        indexWriter.addDocument(doc()
                .add(new NumericField("mvalue").setIntValue(102))
                .build());

        indexWriter.addDocument(doc()
                .add(new NumericField("svalue").setIntValue(4))
                .build());

        IndexReader reader = IndexReader.open(indexWriter, true);

        ShortFieldData sFieldData = ShortFieldData.load(reader, "svalue");
        ShortFieldData mFieldData = ShortFieldData.load(reader, "mvalue");

        assertThat(sFieldData.fieldName(), equalTo("svalue"));
        assertThat(sFieldData.multiValued(), equalTo(false));

        assertThat(mFieldData.fieldName(), equalTo("mvalue"));
        assertThat(mFieldData.multiValued(), equalTo(true));

        // svalue
        assertThat(sFieldData.hasValue(0), equalTo(true));
        assertThat(sFieldData.docFieldData(0).isEmpty(), equalTo(false));
        assertThat(sFieldData.value(0), equalTo((short) 4));
        assertThat(sFieldData.docFieldData(0).getValue(), equalTo((short) 4));
        assertThat(sFieldData.values(0).length, equalTo(1));
        assertThat(sFieldData.docFieldData(0).getValues().length, equalTo(1));
        assertThat(sFieldData.values(0)[0], equalTo((short) 4));
        assertThat(sFieldData.docFieldData(0).getValues()[0], equalTo((short) 4));

        assertThat(sFieldData.hasValue(1), equalTo(true));
        assertThat(sFieldData.value(1), equalTo((short) 3));
        assertThat(sFieldData.values(1).length, equalTo(1));
        assertThat(sFieldData.values(1)[0], equalTo((short) 3));

        assertThat(sFieldData.hasValue(2), equalTo(true));
        assertThat(sFieldData.value(2), equalTo((short) 7));
        assertThat(sFieldData.values(2).length, equalTo(1));
        assertThat(sFieldData.values(2)[0], equalTo((short) 7));

        assertThat(sFieldData.hasValue(3), equalTo(false));

        assertThat(sFieldData.hasValue(4), equalTo(true));
        assertThat(sFieldData.value(4), equalTo((short) 4));
        assertThat(sFieldData.values(4).length, equalTo(1));
        assertThat(sFieldData.values(4)[0], equalTo((short) 4));

        // check order is correct
        final ArrayList<Short> values = new ArrayList<Short>();
        sFieldData.forEachValue(new ShortFieldData.ValueProc() {
            @Override
            public void onValue(short value) {
                values.add(value);
            }
        });
        assertThat(values.size(), equalTo(3));

        assertThat(values.get(0), equalTo((short) 3));
        assertThat(values.get(1), equalTo((short) 4));
        assertThat(values.get(2), equalTo((short) 7));


        // mvalue
        assertThat(mFieldData.hasValue(0), equalTo(true));
        assertThat(mFieldData.value(0), equalTo((short) 104));
        assertThat(mFieldData.values(0).length, equalTo(1));
        assertThat(mFieldData.values(0)[0], equalTo((short) 104));

        assertThat(mFieldData.hasValue(1), equalTo(true));
        assertThat(mFieldData.value(1), equalTo((short) 104));
        assertThat(mFieldData.docFieldData(1).getValue(), equalTo((short) 104));
        assertThat(mFieldData.values(1).length, equalTo(2));
        assertThat(mFieldData.docFieldData(1).getValues().length, equalTo(2));
        assertThat(mFieldData.values(1)[0], equalTo((short) 104));
        assertThat(mFieldData.docFieldData(1).getValues()[0], equalTo((short) 104));
        assertThat(mFieldData.docFieldData(1).getValues()[1], equalTo((short) 105));

        assertThat(mFieldData.hasValue(2), equalTo(false));

        assertThat(mFieldData.hasValue(3), equalTo(true));
        assertThat(mFieldData.value(3), equalTo((short) 102));
        assertThat(mFieldData.values(3).length, equalTo(1));
        assertThat(mFieldData.values(3)[0], equalTo((short) 102));

        assertThat(mFieldData.hasValue(4), equalTo(false));

        indexWriter.close();

        // check order is correct
        values.clear();
        mFieldData.forEachValue(new ShortFieldData.ValueProc() {
            @Override
            public void onValue(short value) {
                values.add(value);
            }
        });
        assertThat(values.size(), equalTo(3));

        assertThat(values.get(0), equalTo((short) 102));
        assertThat(values.get(1), equalTo((short) 104));
        assertThat(values.get(2), equalTo((short) 105));
    }
}