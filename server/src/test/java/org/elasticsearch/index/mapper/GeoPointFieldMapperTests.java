/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.elasticsearch.geometry.utils.Geohash.stringEncode;
import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GeoPointFieldMapperTests extends MapperTestCase {

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "geo_point");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerUpdateCheck(b -> b.field("ignore_z_value", false), m -> {
            GeoPointFieldMapper gpfm = (GeoPointFieldMapper) m;
            assertFalse(gpfm.ignoreZValue());
        });
        checker.registerConflictCheck("null_value", b -> b.field("null_value", "41.12,-71.34"));
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("store", b -> b.field("store", true));
        checker.registerConflictCheck("index", b -> b.field("index", false));
    }

    @Override
    protected Object getSampleValueForDocument() {
        return stringEncode(1.3, 1.2);
    }

    public final void testExistsQueryDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    public void testAggregationsDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertAggregatableConsistency(mapperService.fieldType("field"));
    }

    public void testDimension() throws IOException {
        // Test default setting
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        GeoPointFieldMapper.GeoPointFieldType ft = (GeoPointFieldMapper.GeoPointFieldType) mapperService.fieldType("field");
        assertFalse(ft.isDimension());

        // dimension = false is allowed
        assertDimension(false, GeoPointFieldMapper.GeoPointFieldType::isDimension);

        // dimension = true is not allowed
        Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimension", true);
        })));
        assertThat(e.getCause().getMessage(), containsString("Parameter [time_series_dimension] cannot be set"));
    }

    public void testMetricType() throws IOException {
        // Test default setting
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        GeoPointFieldMapper.GeoPointFieldType ft = (GeoPointFieldMapper.GeoPointFieldType) mapperService.fieldType("field");
        assertNull(ft.getMetricType());

        assertMetricType("position", GeoPointFieldMapper.GeoPointFieldType::getMetricType);

        {
            // Test invalid metric type for this field type
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                minimalMapping(b);
                b.field("time_series_metric", "gauge");
            })));
            assertThat(
                e.getCause().getMessage(),
                containsString("Unknown value [gauge] for field [time_series_metric] - accepted values are [position]")
            );
        }
        {
            // Test invalid metric type for this field type
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                minimalMapping(b);
                b.field("time_series_metric", "counter");
            })));
            assertThat(
                e.getCause().getMessage(),
                containsString("Unknown value [counter] for field [time_series_metric] - accepted values are [position]")
            );
        }
    }

    public final void testPositionMetricType() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_metric", "position");
        }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    public void testTimeSeriesIndexDefault() throws Exception {
        var positionMetricType = TimeSeriesParams.MetricType.POSITION;
        var indexSettings = getIndexSettingsBuilder().put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dimension_field");
        var mapperService = createMapperService(indexSettings.build(), fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_metric", positionMetricType.toString());
        }));
        var ft = (GeoPointFieldMapper.GeoPointFieldType) mapperService.fieldType("field");
        assertThat(ft.getMetricType(), equalTo(positionMetricType));
        assertThat(ft.isIndexed(), is(false));
    }

    public void testMetricAndDocvalues() {
        Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_metric", "position").field("doc_values", false);
        })));
        assertThat(e.getCause().getMessage(), containsString("Field [time_series_metric] requires that [doc_values] is true"));
    }

    public void testMetricAndMultiValues() throws Exception {
        DocumentMapper nonMetricMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        DocumentMapper metricMapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_metric", "position");
        }));
        // Multi-valued test data with various supported formats
        Point pointA = new Point(1, 2);
        Point pointB = new Point(3, 4);
        Object[][] data = new Object[][] {
            new Object[] { WellKnownText.toWKT(pointA), WellKnownText.toWKT(pointB) },
            new Object[] { new Double[] { pointA.getX(), pointA.getY() }, new Double[] { pointB.getX(), pointB.getY() } },
            new Object[] { pointA.getY() + "," + pointA.getX(), pointB.getY() + "," + pointB.getX() },
            new Object[] { GeoJson.toMap(pointA), GeoJson.toMap(pointB) } };
        IndexableField expectedPointA = new LatLonPoint("field", pointA.getY(), pointA.getX());
        IndexableField expectedPointB = new LatLonPoint("field", pointB.getY(), pointB.getX());

        // Verify that metric and non-metric mappers behave the same on single valued fields
        for (Object[] values : data) {
            for (DocumentMapper mapper : new DocumentMapper[] { nonMetricMapper, metricMapper }) {
                ParsedDocument doc = mapper.parse(source(b -> b.field("field", values[0])));
                assertThat(doc.rootDoc().getField("field"), notNullValue());
                IndexableField field = doc.rootDoc().getField("field");
                assertThat(field, instanceOf(LatLonPoint.class));
                assertThat(field.toString(), equalTo(expectedPointA.toString()));
            }
        }

        // Verify that multi-valued fields behave differently for metric and non-metric mappers
        for (Object[] values : data) {
            // Non-metric mapper works with multi-valued data
            {
                ParsedDocument doc = nonMetricMapper.parse(source(b -> b.field("field", values)));
                assertThat(doc.rootDoc().getField("field"), notNullValue());
                Object[] fields = doc.rootDoc()
                    .getFields()
                    .stream()
                    .filter(f -> f.name().equals("field") && f.fieldType().docValuesType() == DocValuesType.NONE)
                    .toArray();
                assertThat(fields.length, equalTo(2));
                assertThat(fields[0], instanceOf(LatLonPoint.class));
                assertThat(fields[0].toString(), equalTo(expectedPointA.toString()));
                assertThat(fields[1], instanceOf(LatLonPoint.class));
                assertThat(fields[1].toString(), equalTo(expectedPointB.toString()));
            }
            // Metric mapper rejects multi-valued data
            {
                Exception e = expectThrows(DocumentParsingException.class, () -> metricMapper.parse(source(b -> b.field("field", values))));
                assertThat(e.getCause().getMessage(), containsString("field type for [field] does not accept more than single value"));
            }
        }
    }

    public void testGeoHashValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", stringEncode(1.3, 1.2))));
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testWKT() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "POINT (2 3)")));
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testLatLonValuesStored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("store", true)));
        ParsedDocument doc = mapper.parse(source(b -> b.startObject("field").field("lat", 1.2).field("lon", 1.3).endObject()));
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testArrayLatLonValues() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "geo_point").field("doc_values", false).field("store", true))
        );
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startArray("field");
            b.startObject().field("lat", 1.2).field("lon", 1.3).endObject();
            b.startObject().field("lat", 1.4).field("lon", 1.5).endObject();
            b.endArray();
        }));

        // doc values are enabled by default, but in this test we disable them; we should only have 2 points
        assertThat(doc.rootDoc().getFields("field"), notNullValue());
        assertThat(doc.rootDoc().getFields("field"), hasSize(4));
    }

    public void testLatLonInOneValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1.2,1.3")));
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testLatLonStringWithZValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("ignore_z_value", true)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1.2,1.3,10.0")));
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testLatLonStringWithZValueException() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("ignore_z_value", false)));
        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.field("field", "1.2,1.3,10.0"))));
        assertThat(e.getCause().getMessage(), containsString("but [ignore_z_value] parameter is [false]"));
    }

    public void testLatLonInOneValueStored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("store", true)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1.2,1.3")));
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testLatLonInOneValueArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "geo_point").field("doc_values", false).field("store", true))
        );
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("field").value("1.2,1.3").value("1.4,1.5").endArray()));

        // doc values are enabled by default, but in this test we disable them; we should only have 2 points
        assertThat(doc.rootDoc().getFields("field"), notNullValue());
        assertThat(doc.rootDoc().getFields("field"), hasSize(4));
    }

    public void testLonLatArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("field").value(1.3).value(1.2).endArray()));
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testLonLatArrayDynamic() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startArray("dynamic_templates");
        {
            mapping.startObject().startObject("point");
            {
                mapping.field("match", "point*");
                mapping.startObject("mapping").field("type", "geo_point").endObject();
            }
            mapping.endObject().endObject();
        }
        mapping.endArray().endObject().endObject();
        DocumentMapper mapper = createDocumentMapper(mapping);

        ParsedDocument doc = mapper.parse(source(b -> b.startArray("point").value(1.3).value(1.2).endArray()));
        assertThat(doc.rootDoc().getField("point"), notNullValue());
    }

    public void testLonLatArrayStored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("store", true)));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("field").value(1.3).value(1.2).endArray()));
        assertThat(doc.rootDoc().getFields("field"), hasSize(3));
    }

    public void testLonLatArrayArrayStored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "geo_point").field("store", true).field("doc_values", false))
        );
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startArray("field");
            b.startArray().value(1.3).value(1.2).endArray();
            b.startArray().value(1.5).value(1.4).endArray();
            b.endArray();
        }));
        assertThat(doc.rootDoc().getFields("field"), notNullValue());
        assertThat(doc.rootDoc().getFields("field"), hasSize(4));
    }

    /**
     * Test that accept_z_value parameter correctly parses
     */
    public void testIgnoreZValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("ignore_z_value", true)));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoPointFieldMapper.class));
        boolean ignoreZValue = ((GeoPointFieldMapper) fieldMapper).ignoreZValue();
        assertThat(ignoreZValue, equalTo(true));

        // explicit false accept_z_value test
        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("ignore_z_value", false)));
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoPointFieldMapper.class));
        ignoreZValue = ((GeoPointFieldMapper) fieldMapper).ignoreZValue();
        assertThat(ignoreZValue, equalTo(false));
    }

    public void testIndexParameter() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("index", false)));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoPointFieldMapper.class));
        assertThat(((GeoPointFieldMapper) fieldMapper).fieldType().isIndexed(), equalTo(false));
        assertThat(((GeoPointFieldMapper) fieldMapper).fieldType().isSearchable(), equalTo(true));
    }

    public void testMultiField() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "geo_point").field("doc_values", false);
            b.startObject("fields");
            {
                b.startObject("geohash").field("type", "keyword").field("doc_values", false).endObject();  // test geohash as keyword
                b.startObject("latlon").field("type", "text").endObject();  // test geohash as text
            }
            b.endObject();
        }));
        LuceneDocument doc = mapper.parse(source(b -> b.field("field", "POINT (2 3)"))).rootDoc();
        assertThat(doc.getFields("field"), hasSize(1));
        assertThat(doc.getField("field"), hasToString(both(containsString("field:2.999")).and(containsString("1.999"))));
        assertThat(doc.getFields("field.geohash"), hasSize(1));
        assertThat(doc.getField("field.geohash").binaryValue().utf8ToString(), equalTo("s093jd0k72s1"));
        assertThat(doc.getFields("field.latlon"), hasSize(1));
        assertThat(doc.getField("field.latlon").stringValue(), equalTo("s093jd0k72s1"));
    }

    public void testMultiFieldWithMultipleValues() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "geo_point").field("doc_values", false);
            b.startObject("fields");
            {
                b.startObject("geohash").field("type", "keyword").field("doc_values", false).endObject();
            }
            b.endObject();
        }));
        LuceneDocument doc = mapper.parse(source(b -> b.array("field", "POINT (2 3)", "POINT (4 5)"))).rootDoc();
        assertThat(doc.getFields("field.geohash"), hasSize(2));
        assertThat(doc.getFields("field.geohash").get(0).binaryValue().utf8ToString(), equalTo("s093jd0k72s1"));
        assertThat(doc.getFields("field.geohash").get(1).binaryValue().utf8ToString(), equalTo("s0fu7n0xng81"));
    }

    public void testKeywordWithGeopointSubfield() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "keyword").field("doc_values", false);
            b.startObject("fields");
            {
                b.startObject("geopoint").field("type", "geo_point").field("doc_values", false).endObject();
            }
            b.endObject();
        }));
        LuceneDocument doc = mapper.parse(source(b -> b.array("field", "s093jd0k72s1"))).rootDoc();
        assertThat(doc.getFields("field"), hasSize(1));
        assertEquals("s093jd0k72s1", doc.getFields("field").get(0).binaryValue().utf8ToString());
        assertThat(doc.getFields("field.geopoint"), hasSize(1));
        assertThat(doc.getField("field.geopoint"), hasToString(both(containsString("field.geopoint:2.999")).and(containsString("1.999"))));
    }

    private static XContentParser documentParser(String value, boolean prettyPrint) throws IOException {
        XContentBuilder docBuilder = JsonXContent.contentBuilder();
        if (prettyPrint) {
            docBuilder.prettyPrint();
        }
        docBuilder.startObject();
        docBuilder.field("field", value);
        docBuilder.endObject();
        String document = Strings.toString(docBuilder);
        XContentParser docParser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, document);
        docParser.nextToken();
        docParser.nextToken();
        assertEquals(XContentParser.Token.VALUE_STRING, docParser.nextToken());
        return docParser;
    }

    public void testGeoHashMultiFieldParser() throws IOException {
        boolean prettyPrint = randomBoolean();
        XContentParser docParser = documentParser("POINT (2 3)", prettyPrint);
        XContentParser expectedParser = documentParser("s093jd0k72s1", prettyPrint);
        XContentParser parser = new GeoPointFieldMapper.GeoHashMultiFieldParser(docParser, "s093jd0k72s1");
        for (int i = 0; i < 10; i++) {
            assertEquals(expectedParser.currentToken(), parser.currentToken());
            assertEquals(expectedParser.currentName(), parser.currentName());
            assertEquals(expectedParser.getTokenLocation(), parser.getTokenLocation());
            assertEquals(expectedParser.textOrNull(), parser.textOrNull());
            expectThrows(UnsupportedOperationException.class, parser::nextToken);
        }
    }

    public void testNullValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point")));
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoPointFieldMapper.class));

        ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
        assertThat(doc.rootDoc().getField("field"), nullValue());
        assertThat(doc.rootDoc().getFields(FieldNamesFieldMapper.NAME), hasSize(0));

        doc = mapper.parse(source(b -> b.startArray("field").value((String) null).endArray()));
        assertThat(doc.rootDoc().getField("field"), nullValue());
        assertThat(doc.rootDoc().getFields(FieldNamesFieldMapper.NAME), hasSize(0));

        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("doc_values", false)));
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoPointFieldMapper.class));

        doc = mapper.parse(source(b -> b.nullField("field")));
        assertThat(doc.rootDoc().getField("field"), nullValue());
        assertThat(doc.rootDoc().getFields(FieldNamesFieldMapper.NAME), hasSize(0));

        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("null_value", "1,2")));
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoPointFieldMapper.class));

        GeoPoint nullValue = ((GeoPointFieldMapper) fieldMapper).nullValue;
        assertThat(nullValue, equalTo(new GeoPoint(1, 2)));

        doc = mapper.parse(source(b -> b.nullField("field")));
        assertThat(doc.rootDoc().getField("field"), notNullValue());
        BytesRef defaultValue = doc.rootDoc().getBinaryValue("field");

        // Shouldn't matter if we specify the value explicitly or use null value
        doc = mapper.parse(source(b -> b.field("field", "1, 2")));
        assertThat(doc.rootDoc().getBinaryValue("field"), equalTo(defaultValue));

        BytesRef threeFour = new LatLonPoint("f", 3, 4).binaryValue();
        doc = mapper.parse(source(b -> b.field("field", "3, 4")));
        assertThat(doc.rootDoc().getBinaryValue("field"), equalTo(threeFour));

        doc = mapper.parse(source(b -> b.startArray("field").nullValue().value("3, 4").endArray()));
        assertMap(
            doc.rootDoc().getFields("field").stream().map(IndexableField::binaryValue).filter(Objects::nonNull).toList(),
            matchesList().item(equalTo(defaultValue)).item(equalTo(threeFour))
        );
    }

    /**
     * Test the fix for a bug that would read the value of field "ignore_z_value" for "ignore_malformed"
     * when setting the "null_value" field. See PR https://github.com/elastic/elasticsearch/pull/49645
     */
    public void testNullValueWithIgnoreMalformed() throws Exception {
        // Set ignore_z_value = false and ignore_malformed = true and test that a malformed point for null_value is normalized.
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                b -> b.field("type", "geo_point")
                    .field("ignore_z_value", false)
                    .field("ignore_malformed", true)
                    .field("null_value", "91,181")
            )
        );
        Mapper fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoPointFieldMapper.class));

        GeoPoint nullValue = ((GeoPointFieldMapper) fieldMapper).nullValue;
        // geo_point [91, 181] should have been normalized to [89, 1]
        assertThat(nullValue, equalTo(new GeoPoint(89, 1)));
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return true;
    }

    @Override
    protected List<ExampleMalformedValue> exampleMalformedValues() {
        return List.of(
            exampleMalformedValue("1234.333").errorMatches("unsupported symbol [.] in geohash [1234.333]"),
            exampleMalformedValue(b -> b.startObject().field("lat", "-").field("lon", 1.3).endObject()).errorMatches(
                "[latitude] must be a valid double value"
            ),
            exampleMalformedValue(b -> b.startObject().field("lat", 1.3).field("lon", "-").endObject()).errorMatches(
                "[longitude] must be a valid double value"
            ),
            exampleMalformedValue("-,1.3").errorMatches("latitude must be a number"),
            exampleMalformedValue("1.3,-").errorMatches("longitude must be a number"),
            exampleMalformedValue(b -> b.startObject().field("lat", "NaN").field("lon", 1.2).endObject()).errorMatches("Required [lat]"),
            exampleMalformedValue(b -> b.startObject().field("lat", 1.2).field("lon", "NaN").endObject()).errorMatches("Required [lon]"),
            exampleMalformedValue("NaN,1.3").errorMatches("invalid latitude NaN; must be between -90.0 and 90.0"),
            exampleMalformedValue("1.3,NaN").errorMatches("invalid longitude NaN; must be between -180.0 and 180.0"),
            exampleMalformedValue(b -> b.startObject().nullField("lat").field("lon", "NaN").endObject()).errorMatches(
                "latitude must be a number"
            ),
            exampleMalformedValue(b -> b.startObject().field("lat", "NaN").nullField("lon").endObject()).errorMatches(
                "longitude must be a number"
            )
        );
    }

    protected void assertSearchable(MappedFieldType fieldType) {
        // always searchable even if it uses TextSearchInfo.NONE
        assertTrue(fieldType.isIndexed());
        assertTrue(fieldType.isSearchable());
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("Test implemented in a follow up", true);
        return null;
    }

    public void testScriptAndPrecludedParameters() {
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                b.field("type", "geo_point");
                b.field("script", "test");
                b.field("ignore_z_value", "true");
            })));
            assertThat(
                e.getMessage(),
                equalTo("Failed to parse mapping: Field [ignore_z_value] cannot be set in conjunction with field [script]")
            );
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                b.field("type", "geo_point");
                b.field("script", "test");
                b.field("null_value", "POINT (1 1)");
            })));
            assertThat(
                e.getMessage(),
                equalTo("Failed to parse mapping: Field [null_value] cannot be set in conjunction with field [script]")
            );
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                b.field("type", "long");
                b.field("script", "test");
                b.field("ignore_malformed", "true");
            })));
            assertThat(
                e.getMessage(),
                equalTo("Failed to parse mapping: Field [ignore_malformed] cannot be set in conjunction with field [script]")
            );
        }
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        assumeFalse("synthetic _source for geo_point doesn't support ignore_malformed", ignoreMalformed);
        return new SyntheticSourceSupport() {
            private final boolean ignoreZValue = usually();
            private final GeoPoint nullValue = usually() ? null : randomGeoPoint();

            @Override
            public SyntheticSourceExample example(int maxVals) {
                if (randomBoolean()) {
                    Tuple<Object, GeoPoint> v = generateValue();
                    return new SyntheticSourceExample(v.v1(), decode(encode(v.v2())), this::mapping);
                }
                List<Tuple<Object, GeoPoint>> values = randomList(1, maxVals, this::generateValue);
                List<Object> in = values.stream().map(Tuple::v1).toList();
                List<Map<String, Object>> outList = values.stream().map(t -> encode(t.v2())).sorted().map(this::decode).toList();
                Object out = outList.size() == 1 ? outList.get(0) : outList;
                return new SyntheticSourceExample(in, out, this::mapping);
            }

            private Tuple<Object, GeoPoint> generateValue() {
                if (nullValue != null && randomBoolean()) {
                    return Tuple.tuple(null, nullValue);
                }
                GeoPoint point = randomGeoPoint();
                return Tuple.tuple(randomGeoPointInput(point), point);
            }

            private GeoPoint randomGeoPoint() {
                Point point = GeometryTestUtils.randomPoint(false);
                return new GeoPoint(point.getLat(), point.getLon());
            }

            private Object randomGeoPointInput(GeoPoint point) {
                if (randomBoolean()) {
                    return Map.of("lat", point.lat(), "lon", point.lon());
                }
                List<Double> coords = new ArrayList<>();
                coords.add(point.lon());
                coords.add(point.lat());
                if (ignoreZValue) {
                    coords.add(randomDouble());
                }
                return Map.of("coordinates", coords, "type", "point");
            }

            private long encode(GeoPoint point) {
                return new LatLonDocValuesField("f", point.lat(), point.lon()).numericValue().longValue();
            }

            private Map<String, Object> decode(long point) {
                double lat = GeoEncodingUtils.decodeLatitude((int) (point >> 32));
                double lon = GeoEncodingUtils.decodeLongitude((int) (point & 0xFFFFFFFF));
                return new TreeMap<>(Map.of("lat", lat, "lon", lon));
            }

            private void mapping(XContentBuilder b) throws IOException {
                b.field("type", "geo_point");
                if (ignoreZValue == false || rarely()) {
                    b.field("ignore_z_value", ignoreZValue);
                }
                if (nullValue != null) {
                    b.field("null_value", randomGeoPointInput(nullValue));
                }
                if (rarely()) {
                    b.field("index", false);
                }
                if (rarely()) {
                    b.field("store", false);
                }
            }

            @Override
            public List<SyntheticSourceInvalidExample> invalidExample() throws IOException {
                return List.of(
                    new SyntheticSourceInvalidExample(
                        equalTo("field [field] of type [geo_point] doesn't support synthetic source because it doesn't have doc values"),
                        b -> b.field("type", "geo_point").field("doc_values", false)
                    ),
                    new SyntheticSourceInvalidExample(
                        equalTo("field [field] of type [geo_point] doesn't support synthetic source because it declares copy_to"),
                        b -> b.field("type", "geo_point").field("copy_to", "foo")
                    ),
                    new SyntheticSourceInvalidExample(
                        equalTo("field [field] of type [geo_point] doesn't support synthetic source because it ignores malformed points"),
                        b -> b.field("type", "geo_point").field("ignore_malformed", true)
                    )
                );
            }
        };
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }
}
