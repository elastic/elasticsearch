/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.XYDocValuesField;
import org.elasticsearch.index.fielddata.AbstractFieldDataImplTestCase;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.index.mapper.PointFieldMapper;

import java.io.IOException;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public abstract class AbstractPointFieldDataTestCase extends AbstractFieldDataImplTestCase {
    @Override
    protected abstract String getFieldDataType();

    protected Field randomPointField(String fieldName, Field.Store store) {
        float x = (float) randomDoubleBetween(-Float.MAX_VALUE, Float.MAX_VALUE, true);
        float y = (float) randomDoubleBetween(-Float.MAX_VALUE, Float.MAX_VALUE, true);
        return new XYDocValuesField(fieldName, x, y);
    }

    @Override
    protected boolean hasDocValues() {
        return true;
    }

    @Override
    protected long minRamBytesUsed() {
        return 0;
    }

    @Override
    protected void fillAllMissing() throws Exception {
        Document d = new Document();
        d.add(new StringField("_id", "1", Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "2", Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "3", Field.Store.NO));
        writer.addDocument(d);
    }

    @Override
    public void testSortMultiValuesFields() {
        assumeFalse("Only test on non point fields", getFieldDataType().equals("point"));
    }

    protected void assertValues(MultiPointValues values, int docId) throws IOException {
        assertValues(values, docId, false);
    }

    protected void assertMissing(MultiPointValues values, int docId) throws IOException {
        assertValues(values, docId, true);
    }

    private void assertValues(MultiPointValues values, int docId, boolean missing) throws IOException {
        assertEquals(missing == false, values.advanceExact(docId));
        if (missing == false) {
            final int docCount = values.docValueCount();
            for (int i = 0; i < docCount; ++i) {
                final CartesianPoint point = values.nextValue();
                assertThat(point.getX(), allOf(greaterThanOrEqualTo(-Float.MAX_VALUE), lessThanOrEqualTo(Float.MAX_VALUE)));
                assertThat(point.getY(), allOf(greaterThanOrEqualTo(-Float.MAX_VALUE), lessThanOrEqualTo(Float.MAX_VALUE)));
            }
        }
    }

    @Override
    public <IFD extends IndexFieldData<?>> IFD getForField(String type, String fieldName, boolean docValues) {
        final MappedFieldType fieldType;
        final Mapper.BuilderContext context = new Mapper.BuilderContext(indexService.getIndexSettings().getSettings(), new ContentPath(1));
        if (type.equals("point")) {
            fieldType = new PointFieldMapper.Builder(fieldName).docValues(docValues).build(context).fieldType();
        } else {
            throw new UnsupportedOperationException(type);
        }
        return shardContext.getForField(fieldType);
    }
}
