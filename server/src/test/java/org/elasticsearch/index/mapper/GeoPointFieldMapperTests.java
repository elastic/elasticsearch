/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.CoreMatchers;

import java.io.IOException;

import static org.elasticsearch.geometry.utils.Geohash.stringEncode;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GeoPointFieldMapperTests extends MapperTestCase {

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "geo_point");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerUpdateCheck(b -> b.field("ignore_malformed", true), m -> {
            GeoPointFieldMapper gpfm = (GeoPointFieldMapper) m;
            assertTrue(gpfm.ignoreMalformed());
        });
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
        assertThat(doc.rootDoc().getFields("field").length, equalTo(4));
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
        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field("field", "1.2,1.3,10.0"))));
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
        assertThat(doc.rootDoc().getFields("field"), arrayWithSize(4));
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
        assertThat(doc.rootDoc().getFields("field").length, equalTo(3));
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
        assertThat(doc.rootDoc().getFields("field").length, CoreMatchers.equalTo(4));
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
        assertThat(doc.getFields("field"), arrayWithSize(1));
        assertThat(doc.getField("field"), hasToString(both(containsString("field:2.999")).and(containsString("1.999"))));
        assertThat(doc.getFields("field.geohash"), arrayWithSize(1));
        assertThat(doc.getField("field.geohash").binaryValue().utf8ToString(), equalTo("s093jd0k72s1"));
        assertThat(doc.getFields("field.latlon"), arrayWithSize(1));
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
        assertThat(doc.getFields("field.geohash"), arrayWithSize(2));
        assertThat(doc.getFields("field.geohash")[0].binaryValue().utf8ToString(), equalTo("s093jd0k72s1"));
        assertThat(doc.getFields("field.geohash")[1].binaryValue().utf8ToString(), equalTo("s0fu7n0xng81"));
    }

    public void testKeywordWithGeopointSubfield() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "keyword").field("doc_values", false);
            ;
            b.startObject("fields");
            {
                b.startObject("geopoint").field("type", "geo_point").field("doc_values", false).endObject();
            }
            b.endObject();
        }));
        LuceneDocument doc = mapper.parse(source(b -> b.array("field", "s093jd0k72s1"))).rootDoc();
        assertThat(doc.getFields("field"), arrayWithSize(1));
        assertEquals("s093jd0k72s1", doc.getFields("field")[0].binaryValue().utf8ToString());
        assertThat(doc.getFields("field.geopoint"), arrayWithSize(1));
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
        assertThat(doc.rootDoc().getFields(FieldNamesFieldMapper.NAME).length, equalTo(0));

        mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("doc_values", false)));
        fieldMapper = mapper.mappers().getMapper("field");
        assertThat(fieldMapper, instanceOf(GeoPointFieldMapper.class));

        doc = mapper.parse(source(b -> b.nullField("field")));
        assertThat(doc.rootDoc().getField("field"), nullValue());
        assertThat(doc.rootDoc().getFields(FieldNamesFieldMapper.NAME).length, equalTo(0));

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
        assertThat(defaultValue, equalTo(doc.rootDoc().getBinaryValue("field")));

        doc = mapper.parse(source(b -> b.field("field", "3, 4")));
        assertThat(defaultValue, not(equalTo(doc.rootDoc().getBinaryValue("field"))));
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

    public void testInvalidGeohashIgnored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("ignore_malformed", "true")));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234.333")));
        assertThat(doc.rootDoc().getField("field"), nullValue());
    }

    public void testInvalidGeohashNotIgnored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.field("field", "1234.333")))
        );
        assertThat(e.getMessage(), containsString("failed to parse field [field] of type [geo_point]"));
        assertThat(e.getRootCause().getMessage(), containsString("unsupported symbol [.] in geohash [1234.333]"));
    }

    public void testInvalidGeopointValuesIgnored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "geo_point").field("ignore_malformed", "true")));

        assertThat(mapper.parse(source(b -> b.field("field", "1234.333"))).rootDoc().getField("field"), nullValue());
        assertThat(
            mapper.parse(source(b -> b.startObject("field").field("lat", "-").field("lon", 1.3).endObject())).rootDoc().getField("field"),
            nullValue()
        );
        assertThat(
            mapper.parse(source(b -> b.startObject("field").field("lat", 1.3).field("lon", "-").endObject())).rootDoc().getField("field"),
            nullValue()
        );
        assertThat(mapper.parse(source(b -> b.field("field", "-,1.3"))).rootDoc().getField("field"), nullValue());
        assertThat(mapper.parse(source(b -> b.field("field", "1.3,-"))).rootDoc().getField("field"), nullValue());
        assertThat(
            mapper.parse(source(b -> b.startObject("field").field("lat", "NaN").field("lon", 1.2).endObject())).rootDoc().getField("field"),
            nullValue()
        );
        assertThat(
            mapper.parse(source(b -> b.startObject("field").field("lat", 1.2).field("lon", "NaN").endObject())).rootDoc().getField("field"),
            nullValue()
        );
        assertThat(mapper.parse(source(b -> b.field("field", "1.3,NaN"))).rootDoc().getField("field"), nullValue());
        assertThat(mapper.parse(source(b -> b.field("field", "NaN,1.3"))).rootDoc().getField("field"), nullValue());
        assertThat(
            mapper.parse(source(b -> b.startObject("field").nullField("lat").field("lon", 1.2).endObject())).rootDoc().getField("field"),
            nullValue()
        );
        assertThat(
            mapper.parse(source(b -> b.startObject("field").field("lat", 1.2).nullField("lon").endObject())).rootDoc().getField("field"),
            nullValue()
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
}
