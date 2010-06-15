/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.field.data.longs;

import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.field.data.FieldData;
import org.testng.annotations.Test;

import java.util.ArrayList;

import static org.elasticsearch.common.collect.Tuple.*;
import static org.elasticsearch.common.lucene.DocumentBuilder.*;
import static org.elasticsearch.index.field.data.FieldDataOptions.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
public class LongFieldDataTests {

    @Test public void intFieldDataTests() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, Lucene.STANDARD_ANALYZER, true, IndexWriter.MaxFieldLength.UNLIMITED);

        indexWriter.addDocument(doc()
                .add(new NumericField("svalue").setLongValue(4))
                .add(new NumericField("mvalue").setLongValue(104))
                .build());

        indexWriter.addDocument(doc()
                .add(new NumericField("svalue").setLongValue(3))
                .add(new NumericField("mvalue").setLongValue(104))
                .add(new NumericField("mvalue").setLongValue(105))
                .build());

        indexWriter.addDocument(doc()
                .add(new NumericField("svalue").setLongValue(7))
                .build());

        indexWriter.addDocument(doc()
                .add(new NumericField("mvalue").setLongValue(102))
                .build());

        indexWriter.addDocument(doc()
                .add(new NumericField("svalue").setLongValue(4))
                .build());

        IndexReader reader = indexWriter.getReader();

        // load it once with no freqs
        LongFieldData.load(reader, "svalue", fieldDataOptions().withFreqs(false));
        LongFieldData.load(reader, "mvalue", fieldDataOptions().withFreqs(false));


        LongFieldData sFieldData = LongFieldData.load(reader, "svalue", fieldDataOptions().withFreqs(true));
        LongFieldData mFieldData = LongFieldData.load(reader, "mvalue", fieldDataOptions().withFreqs(true));

        assertThat(sFieldData.fieldName(), equalTo("svalue"));
        assertThat(sFieldData.type(), equalTo(FieldData.Type.LONG));
        assertThat(sFieldData.multiValued(), equalTo(false));

        assertThat(mFieldData.fieldName(), equalTo("mvalue"));
        assertThat(mFieldData.type(), equalTo(FieldData.Type.LONG));
        assertThat(mFieldData.multiValued(), equalTo(true));

        // svalue
        assertThat(sFieldData.hasValue(0), equalTo(true));
        assertThat(sFieldData.docFieldData(0).isEmpty(), equalTo(false));
        assertThat(sFieldData.value(0), equalTo(4l));
        assertThat(sFieldData.date(0).getMillis(), equalTo(4l));
        assertThat(sFieldData.docFieldData(0).getValue(), equalTo(4l));
        assertThat(sFieldData.values(0).length, equalTo(1));
        assertThat(sFieldData.docFieldData(0).getValues().length, equalTo(1));
        assertThat(sFieldData.values(0)[0], equalTo(4l));
        assertThat(sFieldData.dates(0)[0].getMillis(), equalTo(4l));
        assertThat(sFieldData.docFieldData(0).getValues()[0], equalTo(4l));

        assertThat(sFieldData.hasValue(1), equalTo(true));
        assertThat(sFieldData.value(1), equalTo(3l));
        assertThat(sFieldData.values(1).length, equalTo(1));
        assertThat(sFieldData.values(1)[0], equalTo(3l));

        assertThat(sFieldData.hasValue(2), equalTo(true));
        assertThat(sFieldData.value(2), equalTo(7l));
        assertThat(sFieldData.values(2).length, equalTo(1));
        assertThat(sFieldData.values(2)[0], equalTo(7l));

        assertThat(sFieldData.hasValue(3), equalTo(false));

        assertThat(sFieldData.hasValue(4), equalTo(true));
        assertThat(sFieldData.value(4), equalTo(4l));
        assertThat(sFieldData.values(4).length, equalTo(1));
        assertThat(sFieldData.values(4)[0], equalTo(4l));

        // check order is correct
        final ArrayList<Tuple<Long, Integer>> values = new ArrayList<Tuple<Long, Integer>>();
        sFieldData.forEachValue(new LongFieldData.ValueProc() {
            @Override public void onValue(long value, int freq) {
                values.add(tuple(value, freq));
            }
        });
        assertThat(values.size(), equalTo(3));

        assertThat(values.get(0).v1(), equalTo(3l));
        assertThat(values.get(0).v2(), equalTo(1));

        assertThat(values.get(1).v1(), equalTo(4l));
        assertThat(values.get(1).v2(), equalTo(2));

        assertThat(values.get(2).v1(), equalTo(7l));
        assertThat(values.get(2).v2(), equalTo(1));


        // mvalue
        assertThat(mFieldData.hasValue(0), equalTo(true));
        assertThat(mFieldData.value(0), equalTo(104l));
        assertThat(mFieldData.values(0).length, equalTo(1));
        assertThat(mFieldData.values(0)[0], equalTo(104l));

        assertThat(mFieldData.hasValue(1), equalTo(true));
        assertThat(mFieldData.value(1), equalTo(104l));
        assertThat(mFieldData.date(1).getMillis(), equalTo(104l));
        assertThat(mFieldData.values(1).length, equalTo(2));
        assertThat(mFieldData.values(1)[0], equalTo(104l));
        assertThat(mFieldData.dates(1)[0].getMillis(), equalTo(104l));
        assertThat(mFieldData.values(1)[1], equalTo(105l));
        assertThat(mFieldData.dates(1)[1].getMillis(), equalTo(105l));

        assertThat(mFieldData.hasValue(2), equalTo(false));

        assertThat(mFieldData.hasValue(3), equalTo(true));
        assertThat(mFieldData.value(3), equalTo(102l));
        assertThat(mFieldData.values(3).length, equalTo(1));
        assertThat(mFieldData.values(3)[0], equalTo(102l));

        assertThat(mFieldData.hasValue(4), equalTo(false));

        indexWriter.close();

        // check order is correct
        values.clear();
        mFieldData.forEachValue(new LongFieldData.ValueProc() {
            @Override public void onValue(long value, int freq) {
                values.add(tuple(value, freq));
            }
        });
        assertThat(values.size(), equalTo(3));

        assertThat(values.get(0).v1(), equalTo(102l));
        assertThat(values.get(0).v2(), equalTo(1));

        assertThat(values.get(1).v1(), equalTo(104l));
        assertThat(values.get(1).v2(), equalTo(2));

        assertThat(values.get(2).v1(), equalTo(105l));
        assertThat(values.get(2).v2(), equalTo(1));
    }
}