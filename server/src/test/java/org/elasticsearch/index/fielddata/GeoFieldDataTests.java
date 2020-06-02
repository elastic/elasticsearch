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
import org.apache.lucene.index.Term;
import org.elasticsearch.index.fielddata.plain.AbstractLeafGeoPointFieldData;

import java.util.List;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Basic Unit Test for GeoPointField data
 * todo include backcompat testing - see ISSUE #14562
 */
public class GeoFieldDataTests extends AbstractGeoFieldDataTestCase {
    private static String FIELD_NAME = "value";

    @Override
    protected String getFieldDataType() {
        return "geo_point";
    }

    @Override
    protected void add2SingleValuedDocumentsAndDeleteOneOfThem() throws Exception {
        Document d = new Document();

        d.add(new StringField("_id", "1", Field.Store.NO));
        d.add(randomGeoPointField(FIELD_NAME, Field.Store.YES));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "2", Field.Store.NO));
        d.add(randomGeoPointField(FIELD_NAME, Field.Store.NO));
        writer.addDocument(d);

        writer.commit();

        writer.deleteDocuments(new Term("_id", "1"));
    }

    @Override
    protected void fillMultiValueWithMissing() throws Exception {
        Document d = new Document();
        d.add(new StringField("_id", "1", Field.Store.NO));
        d.add(randomGeoPointField(FIELD_NAME, Field.Store.NO));
        d.add(randomGeoPointField(FIELD_NAME, Field.Store.NO));
        writer.addDocument(d);

        // missing
        d = new Document();
        d.add(new StringField("_id", "2", Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "3", Field.Store.NO));
        d.add(randomGeoPointField(FIELD_NAME, Field.Store.NO));
        writer.addDocument(d);
    }

    @Override
    protected void fillSingleValueAllSet() throws Exception {
        Document d = new Document();
        d.add(new StringField("_id", "1", Field.Store.NO));
        d.add(randomGeoPointField(FIELD_NAME, Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "2", Field.Store.NO));
        d.add(randomGeoPointField(FIELD_NAME, Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "3", Field.Store.NO));
        d.add(randomGeoPointField(FIELD_NAME, Field.Store.NO));
        writer.addDocument(d);
    }

    @Override
    protected void fillSingleValueWithMissing() throws Exception {
        Document d = new Document();
        d.add(new StringField("_id", "1", Field.Store.NO));
        d.add(randomGeoPointField(FIELD_NAME, Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "2", Field.Store.NO));
        //d.add(new StringField("value", one(), Field.Store.NO)); // MISSING....
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "3", Field.Store.NO));
        d.add(randomGeoPointField(FIELD_NAME, Field.Store.NO));
        writer.addDocument(d);
    }

    @Override
    protected void fillMultiValueAllSet() throws Exception {
        Document d = new Document();
        d.add(new StringField("_id", "1", Field.Store.NO));
        d.add(randomGeoPointField(FIELD_NAME, Field.Store.NO));
        d.add(randomGeoPointField(FIELD_NAME, Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "2", Field.Store.NO));
        d.add(randomGeoPointField(FIELD_NAME, Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "3", Field.Store.NO));
        d.add(randomGeoPointField(FIELD_NAME, Field.Store.NO));
        writer.addDocument(d);
    }

    @Override
    protected void fillExtendedMvSet() throws Exception {
        Document d;
        final int maxDocs = randomInt(10);
        for (int i=0; i<maxDocs; ++i) {
            d = new Document();
            d.add(new StringField("_id", i+"", Field.Store.NO));
            int maxVals = randomInt(5);
            for (int v=0; v<maxVals; ++v) {
                d.add(randomGeoPointField(FIELD_NAME, Field.Store.NO));
            }
            writer.addDocument(d);
            if (randomBoolean()) {
                writer.commit();
            }
        }
    }

    @Override
    public void testSingleValueAllSet() throws Exception {
        fillSingleValueAllSet();
        IndexFieldData<?> indexFieldData = getForField("value");
        List<LeafReaderContext> readerContexts = refreshReader();
        for (LeafReaderContext readerContext : readerContexts) {
            LeafFieldData fieldData = indexFieldData.load(readerContext);
            assertThat(fieldData.ramBytesUsed(), greaterThanOrEqualTo(minRamBytesUsed()));

            MultiGeoPointValues fieldValues = ((AbstractLeafGeoPointFieldData)fieldData).getGeoPointValues();
            assertValues(fieldValues, 0);
            assertValues(fieldValues, 1);
            assertValues(fieldValues, 2);
        }
    }

    @Override
    public void testSingleValueWithMissing() throws Exception {
        fillSingleValueWithMissing();
        IndexFieldData<?> indexFieldData = getForField("value");
        List<LeafReaderContext> readerContexts = refreshReader();
        for (LeafReaderContext readerContext : readerContexts) {
            LeafFieldData fieldData = indexFieldData.load(readerContext);
            assertThat(fieldData.ramBytesUsed(), greaterThanOrEqualTo(minRamBytesUsed()));

            MultiGeoPointValues fieldValues = ((AbstractLeafGeoPointFieldData)fieldData).getGeoPointValues();
            assertValues(fieldValues, 0);
            assertMissing(fieldValues, 1);
            assertValues(fieldValues, 2);
        }
    }

    @Override
    public void testMultiValueAllSet() throws Exception {
        fillMultiValueAllSet();
        IndexFieldData<?> indexFieldData = getForField("value");
        List<LeafReaderContext> readerContexts = refreshReader();
        for (LeafReaderContext readerContext : readerContexts) {
            LeafFieldData fieldData = indexFieldData.load(readerContext);
            assertThat(fieldData.ramBytesUsed(), greaterThanOrEqualTo(minRamBytesUsed()));

            MultiGeoPointValues fieldValues = ((AbstractLeafGeoPointFieldData)fieldData).getGeoPointValues();
            assertValues(fieldValues, 0);
            assertValues(fieldValues, 1);
            assertValues(fieldValues, 2);
        }
    }

    @Override
    public void testMultiValueWithMissing() throws Exception {
        fillMultiValueWithMissing();
        IndexFieldData<?> indexFieldData = getForField("value");
        List<LeafReaderContext> readerContexts = refreshReader();
        for (LeafReaderContext readerContext : readerContexts) {
            LeafFieldData fieldData = indexFieldData.load(readerContext);
            assertThat(fieldData.ramBytesUsed(), greaterThanOrEqualTo(minRamBytesUsed()));

            MultiGeoPointValues fieldValues = ((AbstractLeafGeoPointFieldData)fieldData).getGeoPointValues();

            assertValues(fieldValues, 0);
            assertMissing(fieldValues, 1);
            assertValues(fieldValues, 2);
        }
    }
}
