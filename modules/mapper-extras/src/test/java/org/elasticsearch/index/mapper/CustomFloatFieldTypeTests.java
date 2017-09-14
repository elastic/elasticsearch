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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.DoubleToLongFunction;
import java.util.function.LongToDoubleFunction;

public class CustomFloatFieldTypeTests extends FieldTypeTestCase {

    @Override
    protected MappedFieldType createDefaultFieldType() {
        CustomFloatFieldMapper.CustomFloatFieldType ft = new CustomFloatFieldMapper.CustomFloatFieldType();
        ft.setNumSigBits(10);
        ft.setZeroExponent(-20);
        return ft;
    }

    @Before
    public void setupProperties() {
        addModifier(new Modifier("num_significant_bits", false) {
            @Override
            public void modify(MappedFieldType ft) {
                CustomFloatFieldMapper.CustomFloatFieldType tft = (CustomFloatFieldMapper.CustomFloatFieldType)ft;
                tft.setNumSigBits(15);
            }
            @Override
            public void normalizeOther(MappedFieldType other) {
                super.normalizeOther(other);
                ((CustomFloatFieldMapper.CustomFloatFieldType) other).setNumSigBits(10);
            }
        });
        addModifier(new Modifier("zero_exponent", false) {
            @Override
            public void modify(MappedFieldType ft) {
                CustomFloatFieldMapper.CustomFloatFieldType tft = (CustomFloatFieldMapper.CustomFloatFieldType)ft;
                tft.setZeroExponent(10);
            }
            @Override
            public void normalizeOther(MappedFieldType other) {
                super.normalizeOther(other);
                ((CustomFloatFieldMapper.CustomFloatFieldType) other).setZeroExponent(-20);
            }
        });
    }

    public void testTermQuery() {
        CustomFloatFieldMapper.CustomFloatFieldType ft = new CustomFloatFieldMapper.CustomFloatFieldType();
        ft.setName("custom_float");
        ft.setNumSigBits(10);
        ft.setZeroExponent(-20);
        double value = (randomDouble() * 2 - 1) * 10000;
        long encoded = CustomFloat.getEncoder(ft.getNumSigBits(), ft.getZeroExp()).applyAsLong(value);
        assertEquals(LongPoint.newExactQuery("custom_float", encoded), ft.termQuery(value, null));
    }

    public void testTermsQuery() {
        CustomFloatFieldMapper.CustomFloatFieldType ft = new CustomFloatFieldMapper.CustomFloatFieldType();
        ft.setName("custom_float");
        ft.setNumSigBits(10);
        ft.setZeroExponent(-20);
        double value1 = (randomDouble() * 2 - 1) * 10000;
        long encoded1 = CustomFloat.getEncoder(ft.getNumSigBits(), ft.getZeroExp()).applyAsLong(value1);
        double value2 = (randomDouble() * 2 - 1) * 10000;
        long encoded2 = CustomFloat.getEncoder(ft.getNumSigBits(), ft.getZeroExp()).applyAsLong(value2);
        assertEquals(
                LongPoint.newSetQuery("custom_float", encoded1, encoded2),
                ft.termsQuery(Arrays.asList(value1, value2), null));
    }

    public void testRangeQuery() throws IOException {
        // make sure the accuracy loss of scaled floats only occurs at index time
        // this test checks that searching custom floats yields the same results as
        // searching doubles that are rounded to the closest half float
        CustomFloatFieldMapper.CustomFloatFieldType ft = new CustomFloatFieldMapper.CustomFloatFieldType();
        ft.setName("custom_float");
        ft.setNumSigBits(randomIntBetween(2, 10));
        ft.setZeroExponent(randomIntBetween(-10, 2));
        DoubleToLongFunction encoder = CustomFloat.getEncoder(ft.getNumSigBits(), ft.getZeroExp());
        LongToDoubleFunction decoder = CustomFloat.getDecoder(ft.getNumSigBits(), ft.getZeroExp());
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null));
        final int numDocs = 1000;
        for (int i = 0; i < numDocs; ++i) {
            Document doc = new Document();
            double value = (randomDouble() * 2 - 1) * 10000;
            long encodedValue = encoder.applyAsLong(value);
            double rounded = decoder.applyAsDouble(encodedValue);
            doc.add(new LongPoint("custom_float", encodedValue));
            doc.add(new DoublePoint("double", rounded));
            w.addDocument(doc);
        }
        final DirectoryReader reader = DirectoryReader.open(w);
        w.close();
        IndexSearcher searcher = newSearcher(reader);
        final int numQueries = 1000;
        for (int i = 0; i < numQueries; ++i) {
            Double l = randomBoolean() ? null : (randomDouble() * 2 - 1) * 10000;
            Double u = randomBoolean() ? null : (randomDouble() * 2 - 1) * 10000;
            boolean includeLower = randomBoolean();
            boolean includeUpper = randomBoolean();
            Query doubleQ = NumberFieldMapper.NumberType.DOUBLE.rangeQuery("double", l, u, includeLower, includeUpper, false);
            Query customFloatQ = ft.rangeQuery(l, u, includeLower, includeUpper, null);
            assertEquals(searcher.count(doubleQ), searcher.count(customFloatQ));
        }
        IOUtils.close(reader, dir);
    }

    public void testValueForSearch() {
        CustomFloatFieldMapper.CustomFloatFieldType ft = new CustomFloatFieldMapper.CustomFloatFieldType();
        ft.setName("custom_float");
        ft.setNumSigBits(randomIntBetween(2, 10));
        ft.setZeroExponent(randomIntBetween(-10, 2));
        LongToDoubleFunction decoder = CustomFloat.getDecoder(ft.getNumSigBits(), ft.getZeroExp());
        assertNull(ft.valueForDisplay(null));
        assertEquals(decoder.applyAsDouble(10L), ft.valueForDisplay(10L));
    }

    public void testFieldData() throws IOException {
        CustomFloatFieldMapper.CustomFloatFieldType ft = new CustomFloatFieldMapper.CustomFloatFieldType();
        ft.setNumSigBits(randomIntBetween(2, 10));
        ft.setZeroExponent(randomIntBetween(-10, 2));
        LongToDoubleFunction decoder = CustomFloat.getDecoder(ft.getNumSigBits(), ft.getZeroExp());
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null));
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("custom_float1", 10));
        doc.add(new SortedNumericDocValuesField("custom_float2", 5));
        doc.add(new SortedNumericDocValuesField("custom_float2", 12));
        w.addDocument(doc);
        try (DirectoryReader reader = DirectoryReader.open(w)) {
            IndexMetaData indexMetadata = new IndexMetaData.Builder("index").settings(
                    Settings.builder()
                    .put("index.version.created", Version.CURRENT)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0).build()).build();
            IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

            // single-valued
            ft.setName("custom_float1");
            IndexNumericFieldData fielddata = (IndexNumericFieldData) ft.fielddataBuilder("index")
                .build(indexSettings, ft, null, null, null);
            assertEquals(fielddata.getNumericType(), IndexNumericFieldData.NumericType.DOUBLE);
            AtomicNumericFieldData leafFieldData = fielddata.load(reader.leaves().get(0));
            SortedNumericDoubleValues values = leafFieldData.getDoubleValues();
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(decoder.applyAsDouble(10), values.nextValue(), 0d);

            // multi-valued
            ft.setName("custom_float2");
            fielddata = (IndexNumericFieldData) ft.fielddataBuilder("index").build(indexSettings, ft, null, null, null);
            leafFieldData = fielddata.load(reader.leaves().get(0));
            values = leafFieldData.getDoubleValues();
            assertTrue(values.advanceExact(0));
            assertEquals(2, values.docValueCount());
            assertEquals(decoder.applyAsDouble(5), values.nextValue(), 0d);
            assertEquals(decoder.applyAsDouble(12), values.nextValue(), 0d);
        }
        IOUtils.close(w, dir);
    }
}
