/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import com.wdtinc.mapbox_vector_tile.VectorTile;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.mapper.GeoShapeWithDocValuesFieldMapper;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;
import org.hamcrest.Matchers;
import org.locationtech.spatial4j.exception.InvalidShapeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;


public class VectorTileAggregatorTests extends AggregatorTestCase {

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new LocalStateSpatialPlugin());
    }

    public void testGeoPoint() throws Exception {
        int numDocs = scaledRandomIntBetween(64, 256);
        List<Point> points = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            points.add(randomValueOtherThanMany((p) -> p.getLat() > 80 || p.getLat() < -80, GeometryTestUtils::randomPoint));
        }
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (Point point : points) {
                Document document = new Document();
                document.add(new LatLonDocValuesField("field", point.getLat(), point.getLon()));
                BytesRef val = new BytesRef("{\"field\" : \"" + WellKnownText.INSTANCE.toWKT(point) + "\"}");
                document.add(new StoredField("_source", val));
                w.addDocument(document);
            }
            if (randomBoolean()) {
                w.forceMerge(1);
            }
            assertVectorTile(w, 0, 0, numDocs, new GeoPointFieldMapper.GeoPointFieldType("field"));
        }
    }

    @SuppressWarnings("unchecked")
    @AwaitsFix(bugUrl = "That doesn't work yet")
    public void testGeoShape() throws Exception {
        int numDocs = scaledRandomIntBetween(64, 256);
        List<Geometry> geometries = new ArrayList<>();
        GeoShapeIndexer indexer = new GeoShapeIndexer(true, "test");
        for (int i = 0; i < numDocs; i++) {
            Function<Boolean, Geometry> geometryGenerator = ESTestCase.randomFrom(
                GeometryTestUtils::randomLine,
                GeometryTestUtils::randomPoint,
                GeometryTestUtils::randomPolygon,
                GeometryTestUtils::randomMultiLine,
                GeometryTestUtils::randomMultiPolygon
            );
            Geometry geometry = geometryGenerator.apply(false);
            try {
                geometries.add(indexer.prepareForIndexing(geometry));
            } catch (InvalidShapeException e) {
                // do not include geometry
            }
        }
        int expectedPolygons = 0;
        int expectedLines = 0;
        int expectedPoints = 0;
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (Geometry geometry : geometries) {
                Document document = new Document();
                document.add(GeoTestUtils.binaryGeoShapeDocValuesField("field", geometry));
                BytesRef val = new BytesRef("{\"field\" : \"" + WellKnownText.INSTANCE.toWKT(geometry) + "\"}");
                document.add(new StoredField("_source", val));
                w.addDocument(document);
                GeoShapeValues.GeoShapeValue value = GeoTestUtils.geoShapeValue(geometry);
                switch (value.dimensionalShapeType()) {
                    case POINT:
                        if (value.lat() < GeoTileUtils.LATITUDE_MASK && value.lat() > -GeoTileUtils.LATITUDE_MASK) {
                            expectedPoints++;
                        }
                        break;
                    case LINE:
                        expectedLines++;
                        break;
                    case POLYGON:
                        expectedPolygons++;
                        break;
                }
            }
            if (randomBoolean()) {
                w.forceMerge(1);
            }
            MappedFieldType fieldType = new GeoShapeWithDocValuesFieldMapper.GeoShapeWithDocValuesFieldType("field",
                true, true, ShapeBuilder.Orientation.RIGHT, null, Collections.emptyMap());
            assertVectorTile(w, expectedPolygons, expectedLines, expectedPoints, fieldType);
        }
    }

    private void assertVectorTile(RandomIndexWriter w, int expectedPolygons, int expectedLines,
                                  int expectedPoints, MappedFieldType fieldType) throws IOException {
        MappedFieldType sourceFieldType = new SourceFieldMapper.Builder().build().fieldType();
        VectorTileAggregationBuilder aggBuilder = new VectorTileAggregationBuilder("my_agg")
            .field("field");
        try (IndexReader reader = w.getReader()) {
            IndexSearcher searcher = new IndexSearcher(reader);
            InternalVectorTile result = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType, sourceFieldType);
            assertEquals("my_agg", result.getName());
            VectorTile.Tile tileResult = VectorTile.Tile.newBuilder().mergeFrom(result.getVectorTileBytes()).build();
            assertNotNull(tileResult);
            int expectedLayers = (expectedPolygons > 0 ? 1 : 0) + (expectedLines > 0 ? 1 : 0) + (expectedPoints > 0 ? 1 : 0);
            assertThat(expectedLayers, Matchers.equalTo(tileResult.getLayersCount()));
            if (expectedPolygons > 0) {
                VectorTile.Tile.Layer polygons = null;
                for (int i = 0; i < tileResult.getLayersCount(); i++) {
                    VectorTile.Tile.Layer layer = tileResult.getLayers(i);
                    if (AbstractVectorTileAggregator.POLYGON_LAYER.equals(layer.getName())) {
                        polygons = layer;
                        break;
                    }
                }
                assertThat(polygons, Matchers.notNullValue());
                assertThat(AbstractVectorTileAggregator.POLYGON_EXTENT, Matchers.equalTo(polygons.getExtent()));
                assertThat(2, Matchers.equalTo(polygons.getVersion()));
                assertThat(expectedPolygons, Matchers.greaterThanOrEqualTo(polygons.getFeaturesCount()));
            }

            if (expectedLines > 0) {
                VectorTile.Tile.Layer lines = null;
                for (int i = 0; i < tileResult.getLayersCount(); i++) {
                    VectorTile.Tile.Layer layer = tileResult.getLayers(i);
                    if (AbstractVectorTileAggregator.LINE_LAYER.equals(layer.getName())) {
                        lines = layer;
                        break;
                    }
                }
                assertThat(lines, Matchers.notNullValue());
                assertThat(AbstractVectorTileAggregator.LINE_EXTENT, Matchers.equalTo(lines.getExtent()));
                assertThat(2, Matchers.equalTo(lines.getVersion()));
                assertThat(expectedLines, Matchers.greaterThanOrEqualTo(lines.getFeaturesCount()));
            }

            if (expectedPoints > 0) {
                VectorTile.Tile.Layer points = null;
                for (int i = 0; i < tileResult.getLayersCount(); i++) {
                    VectorTile.Tile.Layer layer = tileResult.getLayers(i);
                    if (AbstractVectorTileAggregator.POINT_LAYER.equals(layer.getName())) {
                        points = layer;
                        break;
                    }
                }
                assertThat(points, Matchers.notNullValue());
                assertThat(AbstractVectorTileAggregator.POINT_EXTENT, Matchers.equalTo(points.getExtent()));
                assertThat(2, Matchers.equalTo(points.getVersion()));
                int tagIndex = -1;
                for (int i = 0; i < points.getKeysCount(); i++) {
                    if ("count".equals(points.getKeys(i))) {
                        tagIndex = i;
                        break;
                    }
                }
                assertThat(tagIndex, greaterThanOrEqualTo(0));
                int numberPoints = 0;
                for (int i = 0; i < points.getFeaturesCount(); i++) {
                    VectorTile.Tile.Feature feature = points.getFeatures(i);
                    numberPoints += points.getValues(feature.getTags(tagIndex + 1)).getIntValue();
                }
                assertThat(expectedPoints, Matchers.equalTo(numberPoints));
            }
        }
    }

    public void testInvalidChildAggregation() throws Exception {
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir);
             IndexReader reader = w.getReader()) {
            VectorTileAggregationBuilder aggBuilder = new VectorTileAggregationBuilder("my_agg")
                .field("field");
            GeoGridAggregationBuilder parent = new GeoTileGridAggregationBuilder("my_parent")
                .field("field").subAggregation(aggBuilder);
            MappedFieldType fieldType = new GeoShapeWithDocValuesFieldMapper.GeoShapeWithDocValuesFieldType("field",
                true, true, ShapeBuilder.Orientation.RIGHT, null, Collections.emptyMap());
            IndexSearcher searcher = new IndexSearcher(reader);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, ()
                ->  searchAndReduce(searcher, new MatchAllDocsQuery(), parent, fieldType));
            assertThat(e.getMessage(), equalTo("vector-tile aggregation must be at the top level"));
        }
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new VectorTileAggregationBuilder("foo").field(fieldName).y(0).x(0).z(0);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(CoreValuesSourceType.GEOPOINT, GeoShapeValuesSourceType.instance());
    }
}
