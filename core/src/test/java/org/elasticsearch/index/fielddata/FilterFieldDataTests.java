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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomAccessOrds;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.util.Random;

import static org.hamcrest.Matchers.equalTo;

public class FilterFieldDataTests extends AbstractFieldDataTestCase {

    @Override
    protected FieldDataType getFieldDataType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Test
    public void testFilterByFrequency() throws Exception {
        Random random = getRandom();
        for (int i = 0; i < 1000; i++) {
            Document d = new Document();
            d.add(new StringField("id", "" + i, Field.Store.NO));
            if (i % 100 == 0) {
                d.add(new StringField("high_freq", "100", Field.Store.NO));
                d.add(new StringField("low_freq", "100", Field.Store.NO));
                d.add(new StringField("med_freq", "100", Field.Store.NO));
            }
            if (i % 10 == 0) {
                d.add(new StringField("high_freq", "10", Field.Store.NO));
                d.add(new StringField("med_freq", "10", Field.Store.NO));
            }
            if (i % 5 == 0) {
                d.add(new StringField("high_freq", "5", Field.Store.NO));
            }
            writer.addDocument(d);
        }
        writer.forceMerge(1, true);
        LeafReaderContext context = refreshReader();
        String[] formats = new String[] { "paged_bytes"};
        
        for (String format : formats) {
            {
                ifdService.clear();
                FieldDataType fieldDataType = new FieldDataType("string", Settings.builder().put("format", format)
                        .put("filter.frequency.min_segment_size", 100).put("filter.frequency.min", 0.0d).put("filter.frequency.max", random.nextBoolean() ? 100 : 0.5d));
                IndexOrdinalsFieldData fieldData = getForField(fieldDataType, "high_freq");
                AtomicOrdinalsFieldData loadDirect = fieldData.loadDirect(context);
                RandomAccessOrds bytesValues = loadDirect.getOrdinalsValues();
                assertThat(2L, equalTo(bytesValues.getValueCount()));
                assertThat(bytesValues.lookupOrd(0).utf8ToString(), equalTo("10"));
                assertThat(bytesValues.lookupOrd(1).utf8ToString(), equalTo("100"));
            }
            {
                ifdService.clear();
                FieldDataType fieldDataType = new FieldDataType("string", Settings.builder().put("format", format)
                        .put("filter.frequency.min_segment_size", 100).put("filter.frequency.min",  random.nextBoolean() ? 101 : 101d/200.0d).put("filter.frequency.max", 201));
                IndexOrdinalsFieldData fieldData = getForField(fieldDataType, "high_freq");
                AtomicOrdinalsFieldData loadDirect = fieldData.loadDirect(context);
                RandomAccessOrds bytesValues = loadDirect.getOrdinalsValues();
                assertThat(1L, equalTo(bytesValues.getValueCount()));
                assertThat(bytesValues.lookupOrd(0).utf8ToString(), equalTo("5"));
            }
            
            {
                ifdService.clear(); // test # docs with value
                FieldDataType fieldDataType = new FieldDataType("string", Settings.builder().put("format", format)
                        .put("filter.frequency.min_segment_size", 101).put("filter.frequency.min", random.nextBoolean() ? 101 : 101d/200.0d));
                IndexOrdinalsFieldData fieldData = getForField(fieldDataType, "med_freq");
                AtomicOrdinalsFieldData loadDirect = fieldData.loadDirect(context);
                RandomAccessOrds bytesValues = loadDirect.getOrdinalsValues();
                assertThat(2L, equalTo(bytesValues.getValueCount()));
                assertThat(bytesValues.lookupOrd(0).utf8ToString(), equalTo("10"));
                assertThat(bytesValues.lookupOrd(1).utf8ToString(), equalTo("100"));
            }
            
            {
                ifdService.clear();
                FieldDataType fieldDataType = new FieldDataType("string", Settings.builder().put("format", format)
                        .put("filter.frequency.min_segment_size", 101).put("filter.frequency.min", random.nextBoolean() ? 101 : 101d/200.0d));
                IndexOrdinalsFieldData fieldData = getForField(fieldDataType, "med_freq");
                AtomicOrdinalsFieldData loadDirect = fieldData.loadDirect(context);
                RandomAccessOrds bytesValues = loadDirect.getOrdinalsValues();
                assertThat(2L, equalTo(bytesValues.getValueCount()));
                assertThat(bytesValues.lookupOrd(0).utf8ToString(), equalTo("10"));
                assertThat(bytesValues.lookupOrd(1).utf8ToString(), equalTo("100"));
            }
            
            {
                ifdService.clear();
                FieldDataType fieldDataType = new FieldDataType("string", Settings.builder().put("format", format)
                        .put("filter.regex.pattern", "\\d{2,3}") // allows 10 & 100
                        .put("filter.frequency.min_segment_size", 0)
                        .put("filter.frequency.min", random.nextBoolean() ? 2 : 1d/200.0d) // 100, 10, 5
                        .put("filter.frequency.max", random.nextBoolean() ? 99 : 99d/200.0d)); // 100
                IndexOrdinalsFieldData fieldData = getForField(fieldDataType, "high_freq");
                AtomicOrdinalsFieldData loadDirect = fieldData.loadDirect(context);
                RandomAccessOrds bytesValues = loadDirect.getOrdinalsValues();
                assertThat(1L, equalTo(bytesValues.getValueCount()));
                assertThat(bytesValues.lookupOrd(0).utf8ToString(), equalTo("100"));
            }
        }

    }
    
    @Test
    public void testFilterByRegExp() throws Exception {

        int hundred  = 0;
        int ten  = 0;
        int five  = 0;
        for (int i = 0; i < 1000; i++) {
            Document d = new Document();
            d.add(new StringField("id", "" + i, Field.Store.NO));
            if (i % 100 == 0) {
                hundred++;
                d.add(new StringField("high_freq", "100", Field.Store.NO));
            }
            if (i % 10 == 0) {
                ten++;
                d.add(new StringField("high_freq", "10", Field.Store.NO));
            }
            if (i % 5 == 0) {
                five++;
                d.add(new StringField("high_freq", "5", Field.Store.NO));

            }
            writer.addDocument(d);
        }
        logger.debug(hundred + " " + ten + " " + five);
        writer.forceMerge(1, true);
        LeafReaderContext context = refreshReader();
        String[] formats = new String[] { "paged_bytes"};
        for (String format : formats) {
            {
                ifdService.clear();
                FieldDataType fieldDataType = new FieldDataType("string", Settings.builder().put("format", format)
                        .put("filter.regex.pattern", "\\d"));
                IndexOrdinalsFieldData fieldData = getForField(fieldDataType, "high_freq");
                AtomicOrdinalsFieldData loadDirect = fieldData.loadDirect(context);
                RandomAccessOrds bytesValues = loadDirect.getOrdinalsValues();
                assertThat(1L, equalTo(bytesValues.getValueCount()));
                assertThat(bytesValues.lookupOrd(0).utf8ToString(), equalTo("5"));
            }
            {
                ifdService.clear();
                FieldDataType fieldDataType = new FieldDataType("string", Settings.builder().put("format", format)
                        .put("filter.regex.pattern", "\\d{1,2}"));
                IndexOrdinalsFieldData fieldData = getForField(fieldDataType, "high_freq");
                AtomicOrdinalsFieldData loadDirect = fieldData.loadDirect(context);
                RandomAccessOrds bytesValues = loadDirect.getOrdinalsValues();
                assertThat(2L, equalTo(bytesValues.getValueCount()));
                assertThat(bytesValues.lookupOrd(0).utf8ToString(), equalTo("10"));
                assertThat(bytesValues.lookupOrd(1).utf8ToString(), equalTo("5"));
            }
        }

    }

    @Override
    public void testEmpty() throws Exception {
        // No need to test empty usage here
    }
}
