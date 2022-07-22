/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.fielddata;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.TextFieldMapper;

import java.util.List;
import java.util.Random;

import static org.hamcrest.Matchers.equalTo;

public class FilterFieldDataTests extends AbstractFieldDataTestCase {
    @Override
    protected String getFieldDataType() {
        return null;
    }

    public void testFilterByFrequency() throws Exception {
        Random random = random();
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
        List<LeafReaderContext> contexts = refreshReader();
        final MapperBuilderContext builderContext = MapperBuilderContext.ROOT;

        {
            indexService.clearCaches(false, true);
            MappedFieldType ft = new TextFieldMapper.Builder("high_freq", createDefaultIndexAnalyzers()).fielddata(true)
                .fielddataFrequencyFilter(0, random.nextBoolean() ? 100 : 0.5d, 0)
                .build(builderContext)
                .fieldType();
            IndexOrdinalsFieldData fieldData = searchExecutionContext.getForField(ft, MappedFieldType.FielddataOperation.SEARCH);
            for (LeafReaderContext context : contexts) {
                LeafOrdinalsFieldData loadDirect = fieldData.loadDirect(context);
                SortedSetDocValues bytesValues = loadDirect.getOrdinalsValues();
                assertThat(2L, equalTo(bytesValues.getValueCount()));
                assertThat(bytesValues.lookupOrd(0).utf8ToString(), equalTo("10"));
                assertThat(bytesValues.lookupOrd(1).utf8ToString(), equalTo("100"));
            }
        }
        {
            indexService.clearCaches(false, true);
            MappedFieldType ft = new TextFieldMapper.Builder("high_freq", createDefaultIndexAnalyzers()).fielddata(true)
                .fielddataFrequencyFilter(random.nextBoolean() ? 101 : 101d / 200.0d, 201, 100)
                .build(builderContext)
                .fieldType();
            IndexOrdinalsFieldData fieldData = searchExecutionContext.getForField(ft, MappedFieldType.FielddataOperation.SEARCH);
            for (LeafReaderContext context : contexts) {
                LeafOrdinalsFieldData loadDirect = fieldData.loadDirect(context);
                SortedSetDocValues bytesValues = loadDirect.getOrdinalsValues();
                assertThat(1L, equalTo(bytesValues.getValueCount()));
                assertThat(bytesValues.lookupOrd(0).utf8ToString(), equalTo("5"));
            }
        }

        {
            indexService.clearCaches(false, true);// test # docs with value
            MappedFieldType ft = new TextFieldMapper.Builder("med_freq", createDefaultIndexAnalyzers()).fielddata(true)
                .fielddataFrequencyFilter(random.nextBoolean() ? 101 : 101d / 200.0d, Integer.MAX_VALUE, 101)
                .build(builderContext)
                .fieldType();
            IndexOrdinalsFieldData fieldData = searchExecutionContext.getForField(ft, MappedFieldType.FielddataOperation.SEARCH);
            for (LeafReaderContext context : contexts) {
                LeafOrdinalsFieldData loadDirect = fieldData.loadDirect(context);
                SortedSetDocValues bytesValues = loadDirect.getOrdinalsValues();
                assertThat(2L, equalTo(bytesValues.getValueCount()));
                assertThat(bytesValues.lookupOrd(0).utf8ToString(), equalTo("10"));
                assertThat(bytesValues.lookupOrd(1).utf8ToString(), equalTo("100"));
            }
        }

        {
            indexService.clearCaches(false, true);
            MappedFieldType ft = new TextFieldMapper.Builder("med_freq", createDefaultIndexAnalyzers()).fielddata(true)
                .fielddataFrequencyFilter(random.nextBoolean() ? 101 : 101d / 200.0d, Integer.MAX_VALUE, 101)
                .build(builderContext)
                .fieldType();
            IndexOrdinalsFieldData fieldData = searchExecutionContext.getForField(ft, MappedFieldType.FielddataOperation.SEARCH);
            for (LeafReaderContext context : contexts) {
                LeafOrdinalsFieldData loadDirect = fieldData.loadDirect(context);
                SortedSetDocValues bytesValues = loadDirect.getOrdinalsValues();
                assertThat(2L, equalTo(bytesValues.getValueCount()));
                assertThat(bytesValues.lookupOrd(0).utf8ToString(), equalTo("10"));
                assertThat(bytesValues.lookupOrd(1).utf8ToString(), equalTo("100"));
            }
        }

    }

    @Override
    public void testEmpty() {
        assumeTrue("No need to test empty usage here", false);
    }
}
