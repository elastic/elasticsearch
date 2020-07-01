/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.XYDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.index.mapper.PointFieldMapper;
import org.elasticsearch.xpack.spatial.search.aggregations.support.PointValuesSourceType;
import org.elasticsearch.xpack.spatial.util.ShapeTestUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class PointBoundsAggregatorTests extends AggregatorTestCase {

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new LocalStateSpatialPlugin());
    }

    public void testEmpty() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            BoundsAggregationBuilder aggBuilder = new BoundsAggregationBuilder("my_agg")
                .field("field");

            MappedFieldType fieldType
                = new PointFieldMapper.PointFieldType("field", true, true, Collections.emptyMap());
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalBounds bounds = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertTrue(bounds.box.isUnbounded());
                assertFalse((bounds.topLeft() == null && bounds.bottomRight() == null) == false);
            }
        }
    }

    public void testUnmappedFieldWithDocs() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            if (randomBoolean()) {
                Document doc = new Document();
                doc.add(new XYDocValuesField("field", 0.0f, 0.0f));
                w.addDocument(doc);
            }

            BoundsAggregationBuilder aggBuilder = new BoundsAggregationBuilder("my_agg")
                .field("non_existent");

            MappedFieldType fieldType
                = new PointFieldMapper.PointFieldType("field", true, true, Collections.emptyMap());
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalBounds bounds = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertTrue(bounds.box.isUnbounded());
                assertFalse((bounds.topLeft() == null && bounds.bottomRight() == null) == false);
            }
        }
    }

    public void testMissing() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            doc.add(new NumericDocValuesField("not_field", 1000L));
            w.addDocument(doc);

            MappedFieldType fieldType
                = new PointFieldMapper.PointFieldType("field", true, true, Collections.emptyMap());

            Point point = ShapeTestUtils.randomPoint(false);
            Object missingVal = "POINT(" + point.getX() + " " + point.getY() + ")";

            float x = (float) point.getX();
            float y = (float) point.getY();

            BoundsAggregationBuilder aggBuilder = new BoundsAggregationBuilder("my_agg")
                .field("field")
                .missing(missingVal);

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalBounds bounds = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertFalse(bounds.box.isUnbounded());
                assertThat(bounds.topLeft().getX(), equalTo(x));
                assertThat(bounds.topLeft().getY(), equalTo(y));
                assertThat(bounds.bottomRight().getX(), equalTo(x));
                assertThat(bounds.bottomRight().getY(), equalTo(y));
            }
        }
    }

    public void testInvalidMissing() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            doc.add(new NumericDocValuesField("not_field", 1000L));
            w.addDocument(doc);

            MappedFieldType fieldType
                = new PointFieldMapper.PointFieldType("field", true, true, Collections.emptyMap());

            BoundsAggregationBuilder aggBuilder = new BoundsAggregationBuilder("my_agg")
                .field("field")
                .missing("invalid");

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                // it seems we can return different exceptions depending on the implementation
                ElasticsearchParseException exception = expectThrows(ElasticsearchParseException.class,
                    () -> search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType));
                assertThat(exception.getMessage(), startsWith("failed to parse [[invalid]]"));
            }
        }
    }

    public void testRandomPoints() throws Exception {
        float top = Float.NEGATIVE_INFINITY;
        float bottom = Float.POSITIVE_INFINITY;
        float posLeft = Float.POSITIVE_INFINITY;
        float posRight = Float.NEGATIVE_INFINITY;

        int numDocs = randomIntBetween(50, 100);
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                int numValues = randomIntBetween(1, 5);
                List<Point> points = new ArrayList<>();
                for (int j = 0; j < numValues; j++) {
                    Point point = ShapeTestUtils.randomPoint(false);
                    float x = (float) point.getX();
                    float y = (float) point.getY();
                    points.add(point);
                    if (y  > top) {
                        top = y;
                    }
                    if (y < bottom) {
                        bottom = y;
                    }
                    if (x < posLeft) {
                        posLeft = x;
                    }
                    if (x > posRight) {
                        posRight = x;
                    }
                    doc.add(new XYDocValuesField("field", x, y));
                }
                w.addDocument(doc);
            }
            BoundsAggregationBuilder aggBuilder = new BoundsAggregationBuilder("my_agg")
                .field("field");

            MappedFieldType fieldType
                = new PointFieldMapper.PointFieldType("field", true, true, Collections.emptyMap());

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalBounds bounds = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertFalse(bounds.box.isUnbounded());
                assertThat(bounds.topLeft().getX(), equalTo(posLeft));
                assertThat(bounds.topLeft().getY(), equalTo(top));
                assertThat(bounds.bottomRight().getX(), equalTo(posRight));
                assertThat(bounds.bottomRight().getY(), equalTo(bottom));
                assertTrue((bounds.topLeft() == null && bounds.bottomRight() == null) == false);
            }
        }
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new BoundsAggregationBuilder("foo").field(fieldName);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return singletonList(PointValuesSourceType.instance());
    }
}
