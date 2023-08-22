/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search;

import org.apache.lucene.geo.Circle;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.geo.BoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.index.fielddata.DimensionalShapeType;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.LeafShapeFieldData;
import org.elasticsearch.xpack.spatial.index.fielddata.plain.AbstractAtomicGeoShapeShapeFieldData;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class GeoShapeScriptDocValuesIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(LocalStateSpatialPlugin.class, LocalStateCompositeXPackPlugin.class, CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("lat", this::scriptLat);
            scripts.put("lon", this::scriptLon);
            scripts.put("height", this::scriptHeight);
            scripts.put("width", this::scriptWidth);
            scripts.put("label_lat", this::scriptLabelLat);
            scripts.put("label_lon", this::scriptLabelLon);
            return scripts;
        }

        private double scriptHeight(Map<String, Object> vars) {
            Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
            LeafShapeFieldData.ShapeScriptValues<GeoPoint, GeoShapeValues.GeoShapeValue> geometry = assertGeometry(doc);
            if (geometry.size() == 0) {
                return Double.NaN;
            } else {
                BoundingBox<GeoPoint> boundingBox = geometry.getBoundingBox();
                return boundingBox.topLeft().lat() - boundingBox.bottomRight().lat();
            }
        }

        private double scriptWidth(Map<String, Object> vars) {
            Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
            LeafShapeFieldData.ShapeScriptValues<GeoPoint, GeoShapeValues.GeoShapeValue> geometry = assertGeometry(doc);
            if (geometry.size() == 0) {
                return Double.NaN;
            } else {
                BoundingBox<GeoPoint> boundingBox = geometry.getBoundingBox();
                return boundingBox.bottomRight().lon() - boundingBox.topLeft().lon();
            }
        }

        private double scriptLat(Map<String, Object> vars) {
            Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
            LeafShapeFieldData.ShapeScriptValues<GeoPoint, GeoShapeValues.GeoShapeValue> geometry = assertGeometry(doc);
            return geometry.size() == 0 ? Double.NaN : geometry.getCentroid().lat();
        }

        private double scriptLon(Map<String, Object> vars) {
            Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
            LeafShapeFieldData.ShapeScriptValues<GeoPoint, GeoShapeValues.GeoShapeValue> geometry = assertGeometry(doc);
            return geometry.size() == 0 ? Double.NaN : geometry.getCentroid().lon();
        }

        private double scriptLabelLat(Map<String, Object> vars) {
            Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
            LeafShapeFieldData.ShapeScriptValues<GeoPoint, GeoShapeValues.GeoShapeValue> geometry = assertGeometry(doc);
            return geometry.size() == 0 ? Double.NaN : geometry.getLabelPosition().lat();
        }

        private double scriptLabelLon(Map<String, Object> vars) {
            Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
            LeafShapeFieldData.ShapeScriptValues<GeoPoint, GeoShapeValues.GeoShapeValue> geometry = assertGeometry(doc);
            return geometry.size() == 0 ? Double.NaN : geometry.getLabelPosition().lon();
        }

        private LeafShapeFieldData.ShapeScriptValues<GeoPoint, GeoShapeValues.GeoShapeValue> assertGeometry(Map<?, ?> doc) {
            AbstractAtomicGeoShapeShapeFieldData.GeoShapeScriptValues geometry =
                (AbstractAtomicGeoShapeShapeFieldData.GeoShapeScriptValues) doc.get("location");
            if (geometry.size() == 0) {
                assertThat(geometry.getBoundingBox(), Matchers.nullValue());
                assertThat(geometry.getCentroid(), Matchers.nullValue());
                assertThat(geometry.getLabelPosition(), Matchers.nullValue());
                assertThat(geometry.getDimensionalType(), equalTo(-1));
            } else {
                assertThat(geometry.getBoundingBox(), Matchers.notNullValue());
                assertThat(geometry.getCentroid(), Matchers.notNullValue());
                assertThat(geometry.getLabelPosition(), Matchers.notNullValue());
                assertThat(geometry.getDimensionalType(), greaterThanOrEqualTo(0));
                assertThat(geometry.getDimensionalType(), lessThanOrEqualTo(2));
            }
            return geometry;
        }
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @Before
    public void setupTestIndex() throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("location")
            .field("type", "geo_shape");
        xContentBuilder.endObject().endObject().endObject().endObject();
        assertAcked(indicesAdmin().prepareCreate("test").setMapping(xContentBuilder));
        ensureGreen();
    }

    public void testRandomShape() throws Exception {
        GeoShapeIndexer indexer = new GeoShapeIndexer(Orientation.CCW, "test");
        Geometry geometry = randomValueOtherThanMany(g -> {
            try {
                indexer.indexShape(g);
                return false;
            } catch (Exception e) {
                return true;
            }
        }, () -> GeometryTestUtils.randomGeometry(false));
        doTestGeometry(geometry);
    }

    public void testPolygonFromYamlTests() throws IOException, ParseException {
        // This is the geometry used in the tests in 70_script_doc_values.yml, and is easier to test and debug here
        String wkt = "POLYGON(("
            + "24.04725 59.942,24.04825 59.94125,24.04875 59.94125,24.04875 59.94175,24.048 59.9425,"
            + "24.0475 59.94275,24.0465 59.94225,24.046 59.94225,24.04575 59.9425,24.04525 59.94225,24.04725 59.942"
            + "))";
        Geometry polygon = WellKnownText.fromWKT(GeographyValidator.instance(true), true, wkt);
        doTestGeometry(polygon, null);
    }

    public void testPolygonDateline() throws Exception {
        Geometry geometry = new Polygon(new LinearRing(new double[] { 170, 190, 190, 170, 170 }, new double[] { -5, -5, 5, 5, -5 }));
        doTestGeometry(geometry, null);
    }

    private MultiPoint pointsFromLine(Line line) {
        ArrayList<Point> points = new ArrayList<>();
        for (int i = 0; i < line.length(); i++) {
            double x = line.getX(i);
            double y = line.getY(i);
            points.add(new Point(x, y));
        }
        return new MultiPoint(points);
    }

    public void testEvenLineString() throws Exception {
        Line line = new Line(new double[] { -5, -1, 0, 1, 5 }, new double[] { 0, 0, 0, 0, 0 });
        doTestGeometry(line, GeoTestUtils.geoShapeValue(new Point(-0.5, 0)));
        doTestGeometry(pointsFromLine(line), GeoTestUtils.geoShapeValue(new Point(0, 0)));
    }

    public void testOddLineString() throws Exception {
        Line line = new Line(new double[] { -5, -1, 1, 5 }, new double[] { 0, 0, 0, 0 });
        doTestGeometry(line, GeoTestUtils.geoShapeValue(new Point(0, 0)));
        doTestGeometry(pointsFromLine(line), GeoTestUtils.geoShapeValue(new Point(-1, 0)));
    }

    public void testUnbalancedEvenLineString() throws Exception {
        Line line = new Line(new double[] { -5, -4, -3, -2, -1, 0, 5 }, new double[] { 0, 0, 0, 0, 0, 0, 0 });
        doTestGeometry(line, GeoTestUtils.geoShapeValue(new Point(-2.5, 0)));
        doTestGeometry(pointsFromLine(line), GeoTestUtils.geoShapeValue(new Point(-2, 0)));
    }

    public void testUnbalancedOddLineString() throws Exception {
        Line line = new Line(new double[] { -5, -4, -3, -2, -1, 5 }, new double[] { 0, 0, 0, 0, 0, 0 });
        doTestGeometry(line, GeoTestUtils.geoShapeValue(new Point(-2.5, 0)));
        doTestGeometry(pointsFromLine(line), GeoTestUtils.geoShapeValue(new Point(-3, 0)));
    }

    public void testVerticalLineString() throws Exception {
        // Data with no x-range is not well sorted and odd choices occur for the first triangle tree node
        Line line = new Line(new double[] { 0, 0, 0, 0, 0 }, new double[] { -5, -1, 0, 1, 5 });
        doTestGeometry(line, GeoTestUtils.geoShapeValue(new Point(0, 3)));
        doTestGeometry(pointsFromLine(line), GeoTestUtils.geoShapeValue(new Point(0, 1)));
    }

    public void testOffVerticalLineString() throws Exception {
        // Even a very small x-range results in reasonable sorting for the label position
        Line line = new Line(new double[] { -0.0005, -0.0001, 0, 0.0001, 0.0005 }, new double[] { -5, -1, 0, 1, 5 });
        doTestGeometry(line, GeoTestUtils.geoShapeValue(new Point(-0.00005, -0.5)));
        doTestGeometry(pointsFromLine(line), GeoTestUtils.geoShapeValue(new Point(0, 0)));
    }

    private void doTestGeometry(Geometry geometry) throws IOException {
        doTestGeometry(geometry, null, false);
    }

    private void doTestGeometry(Geometry geometry, GeoShapeValues.GeoShapeValue expectedLabelPosition) throws IOException {
        doTestGeometry(geometry, expectedLabelPosition, true);
    }

    private void doTestGeometry(Geometry geometry, GeoShapeValues.GeoShapeValue expectedLabelPosition, boolean fallbackToCentroid)
        throws IOException {
        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject().field("name", "TestPosition").field("location", WellKnownText.toWKT(geometry)).endObject()
            )
            .get();

        indicesAdmin().prepareRefresh("test").get();

        GeoShapeValues.GeoShapeValue value = GeoTestUtils.geoShapeValue(geometry);

        SearchResponse searchResponse = client().prepareSearch()
            .addStoredField("_source")
            .addScriptField("lat", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "lat", Collections.emptyMap()))
            .addScriptField("lon", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "lon", Collections.emptyMap()))
            .addScriptField("height", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "height", Collections.emptyMap()))
            .addScriptField("width", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "width", Collections.emptyMap()))
            .addScriptField("label_lat", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "label_lat", Collections.emptyMap()))
            .addScriptField("label_lon", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "label_lon", Collections.emptyMap()))
            .get();
        assertSearchResponse(searchResponse);
        Map<String, DocumentField> fields = searchResponse.getHits().getHits()[0].getFields();
        assertThat(fields.get("lat").getValue(), equalTo(value.getY()));
        assertThat(fields.get("lon").getValue(), equalTo(value.getX()));
        assertThat(fields.get("height").getValue(), equalTo(value.boundingBox().maxY() - value.boundingBox().minY()));
        assertThat(fields.get("width").getValue(), equalTo(value.boundingBox().maxX() - value.boundingBox().minX()));

        // Check label position is in the geometry, but with a tolerance constructed as a circle of 1m radius to handle quantization
        Point labelPosition = new Point(fields.get("label_lon").getValue(), fields.get("label_lat").getValue());
        Circle tolerance = new Circle(labelPosition.getY(), labelPosition.getX(), 1);
        assertTrue(
            "Expect label position " + labelPosition + " to intersect geometry " + geometry,
            value.relate(tolerance) != GeoRelation.QUERY_DISJOINT
        );

        // Check that the label position is the expected one, or the centroid in certain polygon cases
        if (expectedLabelPosition != null) {
            doTestLabelPosition(fields, expectedLabelPosition);
        } else if (fallbackToCentroid && value.dimensionalShapeType() == DimensionalShapeType.POLYGON) {
            // Use the centroid for all polygons, unless overwritten for specific cases
            doTestLabelPosition(fields, GeoTestUtils.geoShapeValue(new Point(value.getX(), value.getY())));
        }
    }

    private void doTestLabelPosition(Map<String, DocumentField> fields, GeoShapeValues.GeoShapeValue expectedLabelPosition)
        throws IOException {
        assertEquals(
            "Unexpected latitude for label position,",
            expectedLabelPosition.getY(),
            fields.get("label_lat").getValue(),
            0.0000001
        );
        assertEquals(
            "Unexpected longitude for label position,",
            expectedLabelPosition.getX(),
            fields.get("label_lon").getValue(),
            0.0000001
        );
    }

    public void testNullShape() throws Exception {
        client().prepareIndex("test")
            .setId("1")
            .setSource(jsonBuilder().startObject().field("name", "TestPosition").nullField("location").endObject())
            .get();

        indicesAdmin().prepareRefresh("test").get();

        SearchResponse searchResponse = client().prepareSearch()
            .addStoredField("_source")
            .addScriptField("lat", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "lat", Collections.emptyMap()))
            .addScriptField("lon", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "lon", Collections.emptyMap()))
            .addScriptField("height", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "height", Collections.emptyMap()))
            .addScriptField("width", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "width", Collections.emptyMap()))
            .get();
        assertSearchResponse(searchResponse);
        Map<String, DocumentField> fields = searchResponse.getHits().getHits()[0].getFields();
        assertThat(fields.get("lat").getValue(), equalTo(Double.NaN));
        assertThat(fields.get("lon").getValue(), equalTo(Double.NaN));
        assertThat(fields.get("height").getValue(), equalTo(Double.NaN));
        assertThat(fields.get("width").getValue(), equalTo(Double.NaN));
    }
}
