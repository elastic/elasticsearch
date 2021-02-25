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
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.index.mapper.GeoShapeWithDocValuesFieldMapper;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;
import org.elasticsearch.xpack.spatial.vectortile.FeatureFactory;
import org.hamcrest.Matchers;
import org.locationtech.spatial4j.exception.InvalidShapeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;


public class VectorTileAggregatorTests extends AggregatorTestCase {

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new LocalStateSpatialPlugin());
    }

    public void testGeoPoint() throws Exception {
        int numDocs = scaledRandomIntBetween(64, 256);
        List<Point> points = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            points.add(GeometryTestUtils.randomPoint());
        }
        final VectorTile.Tile.Layer.Builder layerBuilder = VectorTile.Tile.Layer.newBuilder();
        layerBuilder.setVersion(2);
        layerBuilder.setExtent(256);
        layerBuilder.setName("my_agg");
        // TODO: It would be nice to use different tiles?
        final FeatureFactory factory = new FeatureFactory(0, 0, 0, 256);
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (Point point : points) {
                Document document = new Document();
                document.add(new LatLonDocValuesField("field", point.getLat(), point.getLon()));
                BytesRef val = new BytesRef("{\"field\" : \"" + WellKnownText.INSTANCE.toWKT(point) + "\"}");
                document.add(new StoredField("_source", val));
                w.addDocument(document);
                List<VectorTile.Tile.Feature> features = factory.getFeatures(point);
                for (VectorTile.Tile.Feature feature : features) {
                    layerBuilder.addFeatures(feature);
                }
            }
            // force using a single aggregator to compute the centroid
            w.forceMerge(1);
            final VectorTile.Tile.Builder tileBuilder = VectorTile.Tile.newBuilder();
            // Build MVT layer
            final VectorTile.Tile.Layer layer = layerBuilder.build();
            // Add built layer to MVT
            tileBuilder.addLayers(layer);
            VectorTile.Tile expecteTile = tileBuilder.build();
            assertVectorTile(w, expecteTile, new GeoPointFieldMapper.GeoPointFieldType("field"));
        }
    }

    @SuppressWarnings("unchecked")
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
                GeometryTestUtils::randomMultiPoint,
                GeometryTestUtils::randomMultiPolygon
            );
            Geometry geometry = geometryGenerator.apply(false);
            try {
                indexer.prepareForIndexing(geometry);
                geometries.add(geometry);
                // geometries.add(indexer.prepareForIndexing(geometry));
            } catch (InvalidShapeException e) {
                // do not include geometry
            }
        }
        final VectorTile.Tile.Layer.Builder layerBuilder = VectorTile.Tile.Layer.newBuilder();
        layerBuilder.setVersion(2);
        layerBuilder.setName("my_agg");
        // TODO: It would be nice to use different tiles?
        final FeatureFactory factory = new FeatureFactory(0, 0, 0, 4096);
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (Geometry geometry : geometries) {
                Document document = new Document();
                document.add(GeoTestUtils.binaryGeoShapeDocValuesField("field", geometry));
                BytesRef val = new BytesRef("{\"field\" : \"" + WellKnownText.INSTANCE.toWKT(geometry) + "\"}");
                document.add(new StoredField("_source", val));
                w.addDocument(document);
                List<VectorTile.Tile.Feature> features = factory.getFeatures(geometry);
                for (VectorTile.Tile.Feature feature : features) {
                    layerBuilder.addFeatures(feature);
                }
            }
            // force using a single aggregator to compute the centroid
            w.forceMerge(1);
            final VectorTile.Tile.Builder tileBuilder = VectorTile.Tile.newBuilder();
            // Build MVT layer
            final VectorTile.Tile.Layer layer = layerBuilder.build();
            // Add built layer to MVT
            tileBuilder.addLayers(layer);
            VectorTile.Tile expectedtile = tileBuilder.build();
            MappedFieldType fieldType = new GeoShapeWithDocValuesFieldMapper.GeoShapeWithDocValuesFieldType("field",
                true, true, ShapeBuilder.Orientation.RIGHT, null, Collections.emptyMap());
            assertVectorTile(w, expectedtile, fieldType);
        }
    }

    private void assertVectorTile(RandomIndexWriter w, VectorTile.Tile tile, MappedFieldType fieldType) throws IOException {
        MappedFieldType sourceFieldType = new SourceFieldMapper.Builder().build().fieldType();
        VectorTileAggregationBuilder aggBuilder = new VectorTileAggregationBuilder("my_agg")
            .field("field");
        try (IndexReader reader = w.getReader()) {
            IndexSearcher searcher = new IndexSearcher(reader);
            InternalVectorTile result = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType, sourceFieldType);
            assertEquals("my_agg", result.getName());
            VectorTile.Tile tileResult = VectorTile.Tile.newBuilder().mergeFrom(result.getVectorTile()).build();
            assertNotNull(tileResult);
            assertThat(tile.getLayersCount(), Matchers.equalTo(tileResult.getLayersCount()));
            assertThat(tile.getLayers(0).getExtent(), Matchers.equalTo(tileResult.getLayers(0).getExtent()));
            assertThat(tile.getLayers(0).getName(), Matchers.equalTo(tileResult.getLayers(0).getName()));
            assertThat(tile.getLayers(0).getVersion(), Matchers.equalTo(tileResult.getLayers(0).getVersion()));
            assertThat(tile.getLayers(0).getFeaturesCount(), Matchers.equalTo(tileResult.getLayers(0).getFeaturesCount()));
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
