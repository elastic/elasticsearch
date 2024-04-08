/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.XYPointField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.geometry.utils.Geohash.stringEncode;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class PointFieldMapperTests extends CartesianFieldMapperTests {

    @Override
    protected String getFieldName() {
        return "point";
    }

    @Override
    protected void assertXYPointField(IndexableField field, float x, float y) {
        // Unfortunately XYPointField and parent classes like IndexableField do not define equals, so we use toString
        assertThat(field.toString(), is(new XYPointField(FIELD_NAME, 2000.1f, 305.6f).toString()));
    }

    /** The GeoJSON parser used by 'point' and 'geo_point' mimic the required fields of the GeoJSON parser */
    @Override
    protected void assertGeoJSONParseException(DocumentParsingException e, String missingField) {
        assertThat(e.getMessage(), containsString("failed to parse"));
        assertThat(e.getCause().getMessage(), containsString("Required [" + missingField + "]"));
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerUpdateCheck(b -> b.field("ignore_z_value", false), m -> {
            PointFieldMapper gpfm = (PointFieldMapper) m;
            assertFalse(gpfm.ignoreZValue());
        });
    }

    public void testAggregationsDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertAggregatableConsistency(mapperService.fieldType("field"));
    }

    public void testValuesStored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "point");
            b.field("store", true);
        }));
        SourceToParse sourceToParse = source(b -> b.startObject(FIELD_NAME).field("x", 2000.1).field("y", 305.6).endObject());
        ParsedDocument doc = mapper.parse(sourceToParse);
        assertThat(doc.rootDoc().getField(FIELD_NAME), notNullValue());
    }

    public void testArrayValues() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "point");
            b.field("doc_values", false);
            b.field("store", true);
        }));

        SourceToParse sourceToParse = source(
            b -> b.startArray(FIELD_NAME)
                .startObject()
                .field("x", 1.2)
                .field("y", 1.3)
                .endObject()
                .startObject()
                .field("x", 1.4)
                .field("y", 1.5)
                .endObject()
                .endArray()
        );
        ParsedDocument doc = mapper.parse(sourceToParse);

        // doc values are enabled by default, but in this test we disable them; we should only have 2 points
        assertThat(doc.rootDoc().getFields(FIELD_NAME), notNullValue());
        assertThat(doc.rootDoc().getFields(FIELD_NAME), hasSize(4));
    }

    public void testXYInOneValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        SourceToParse sourceToParse = source(b -> b.field(FIELD_NAME, "1.2,1.3"));
        ParsedDocument doc = mapper.parse(sourceToParse);
        assertThat(doc.rootDoc().getField(FIELD_NAME), notNullValue());
    }

    public void testInOneValueStored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "point");
            b.field("store", true);
        }));
        SourceToParse sourceToParse = source(b -> b.field(FIELD_NAME, "1.2,1.3"));
        ParsedDocument doc = mapper.parse(sourceToParse);
        assertThat(doc.rootDoc().getField(FIELD_NAME), notNullValue());
    }

    public void testXYInOneValueArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "point");
            b.field("doc_values", false);
            b.field("store", true);
        }));
        SourceToParse sourceToParse = source(b -> b.startArray(FIELD_NAME).value("1.2,1.3").value("1.4,1.5").endArray());
        ParsedDocument doc = mapper.parse(sourceToParse);

        // doc values are enabled by default, but in this test we disable them; we should only have 2 points
        assertThat(doc.rootDoc().getFields(FIELD_NAME), notNullValue());
        assertThat(doc.rootDoc().getFields(FIELD_NAME), hasSize(4));
    }

    public void testArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        SourceToParse sourceToParse = source(b -> b.startArray(FIELD_NAME).value(1.3).value(1.2).endArray());
        ParsedDocument doc = mapper.parse(sourceToParse);
        assertThat(doc.rootDoc().getField(FIELD_NAME), notNullValue());
    }

    public void testArrayDynamic() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startArray("dynamic_templates");
        {
            mapping.startObject().startObject("point");
            {
                mapping.field("match", "point*");
                mapping.startObject("mapping").field("type", "point").endObject();
            }
            mapping.endObject().endObject();
        }
        mapping.endArray().endObject().endObject();
        DocumentMapper mapper = createDocumentMapper(mapping);

        ParsedDocument doc = mapper.parse(source(b -> b.startArray("point").value(1.3).value(1.2).endArray()));
        assertThat(doc.rootDoc().getField("point"), notNullValue());
    }

    public void testArrayStored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "point");
            b.field("store", true);
        }));
        SourceToParse sourceToParse = source(b -> b.startArray(FIELD_NAME).value(1.3).value(1.2).endArray());
        ParsedDocument doc = mapper.parse(sourceToParse);
        assertThat(doc.rootDoc().getField(FIELD_NAME), notNullValue());
        assertThat(doc.rootDoc().getFields(FIELD_NAME), hasSize(3));
    }

    public void testArrayArrayStored() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "point");
            b.field("store", true);
            b.field("doc_values", false);
        }));

        SourceToParse sourceToParse = source(
            b -> b.startArray(FIELD_NAME)
                .startArray()
                .value(1.3)
                .value(1.2)
                .endArray()
                .startArray()
                .value(1.5)
                .value(1.4)
                .endArray()
                .endArray()
        );
        ParsedDocument doc = mapper.parse(sourceToParse);

        assertThat(doc.rootDoc().getFields(FIELD_NAME), notNullValue());
        assertThat(doc.rootDoc().getFields(FIELD_NAME), hasSize(4));
    }

    public void testNullValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "point");
            b.field("null_value", "1,2");
        }));

        Mapper fieldMapper = mapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(PointFieldMapper.class));

        Object nullValue = ((PointFieldMapper) fieldMapper).getNullValue();
        assertThat(nullValue, equalTo(new CartesianPoint(1, 2)));

        ParsedDocument doc = mapper.parse(source(b -> b.nullField(FIELD_NAME)));

        assertThat(doc.rootDoc().getField(FIELD_NAME), notNullValue());
        BytesRef defaultValue = doc.rootDoc().getBinaryValue(FIELD_NAME);

        doc = mapper.parse(source(b -> b.field(FIELD_NAME, "1, 2")));
        // Shouldn't matter if we specify the value explicitly or use null value
        assertThat(defaultValue, equalTo(doc.rootDoc().getBinaryValue(FIELD_NAME)));

        doc = mapper.parse(source(b -> b.field(FIELD_NAME, "3, 4")));
        // Shouldn't matter if we specify the value explicitly or use null value
        assertThat(defaultValue, not(equalTo(doc.rootDoc().getBinaryValue(FIELD_NAME))));
    }

    public void testInvalidPointValues() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.field(FIELD_NAME, "1234.333"))));
        assertThat(e.getMessage(), containsString("failed to parse"));
        assertThat(e.getCause().getMessage(), containsString("expected 2 or 3 coordinates"));

        e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject(FIELD_NAME).field("geohash", stringEncode(0, 0)).endObject()))
        );
        assertThat(e.getMessage(), containsString("failed to parse"));
        assertThat(e.getCause().getMessage(), containsString("field [geohash] not supported - must be one of: x, y, z, type, coordinates"));

        e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject(FIELD_NAME).field("x", 1.3).field("y", "-").endObject()))
        );
        assertThat(e.getMessage(), containsString("failed to parse"));
        assertThat(e.getCause().getMessage(), containsString("[y] must be a valid double value"));

        e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject(FIELD_NAME).field("x", "-").field("y", 1.3).endObject()))
        );
        assertThat(e.getMessage(), containsString("failed to parse"));
        assertThat(e.getCause().getMessage(), containsString("[x] must be a valid double value"));

        e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.field(FIELD_NAME, "-,1.3"))));
        assertThat(e.getMessage(), containsString("failed to parse"));
        assertThat(e.getCause().getMessage(), containsString("[x] must be a number"));

        e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.field(FIELD_NAME, "1.3,-"))));
        assertThat(e.getMessage(), containsString("failed to parse"));
        assertThat(e.getCause().getMessage(), containsString("[y] must be a number"));

        e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject(FIELD_NAME).field("lon", 1.3).field("y", 1.3).endObject()))
        );
        assertThat(e.getMessage(), containsString("failed to parse"));
        assertThat(e.getCause().getMessage(), containsString("field [lon] not supported - must be one of: x, y, z, type, coordinates"));

        e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject(FIELD_NAME).field("x", 1.3).field("lat", 1.3).endObject()))
        );
        assertThat(e.getMessage(), containsString("failed to parse"));
        assertThat(e.getCause().getMessage(), containsString("field [lat] not supported - must be one of: x, y, z, type, coordinates"));

        e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject(FIELD_NAME).field("x", "NaN").field("y", "NaN").endObject()))
        );
        assertThat(e.getMessage(), containsString("failed to parse"));
        assertThat(e.getCause().getMessage(), containsString("field must be either lat/lon or type/coordinates"));

        e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject(FIELD_NAME).field("x", "NaN").field("y", 1.3).endObject()))
        );
        assertThat(e.getMessage(), containsString("failed to parse"));
        assertThat(e.getCause().getMessage(), containsString("Required [x]"));

        e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject(FIELD_NAME).field("x", 1.3).field("y", "NaN").endObject()))
        );
        assertThat(e.getMessage(), containsString("failed to parse"));
        assertThat(e.getCause().getMessage(), containsString("Required [y]"));

        e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.field(FIELD_NAME, "NaN,NaN"))));
        assertThat(e.getMessage(), containsString("failed to parse"));
        assertThat(e.getCause().getMessage(), containsString("invalid [x] value [NaN]"));

        e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.field(FIELD_NAME, "10,NaN"))));
        assertThat(e.getMessage(), containsString("failed to parse"));
        assertThat(e.getCause().getMessage(), containsString("invalid [y] value [NaN]"));

        e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.field(FIELD_NAME, "NaN,12"))));
        assertThat(e.getMessage(), containsString("failed to parse"));
        assertThat(e.getCause().getMessage(), containsString("invalid [x] value [NaN]"));

        e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject(FIELD_NAME).field("x", 1.3).nullField("y").endObject()))
        );
        assertThat(e.getMessage(), containsString("failed to parse"));
        assertThat(e.getCause().getMessage(), containsString("y must be a number"));

        e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject(FIELD_NAME).nullField("x").field("y", 1.3).endObject()))
        );
        assertThat(e.getMessage(), containsString("failed to parse"));
        assertThat(e.getCause().getMessage(), containsString("x must be a number"));
    }

    /**
     * Test that accept_z_value parameter correctly parses
     */
    public void testIgnoreZValue() throws IOException {
        {
            // explicit true accept_z_value test
            DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
                b.field("type", "point");
                b.field("ignore_z_value", true);
            }));
            Mapper fieldMapper = mapper.mappers().getMapper(FIELD_NAME);
            assertThat(fieldMapper, instanceOf(PointFieldMapper.class));
            boolean ignoreZValue = ((PointFieldMapper) fieldMapper).ignoreZValue();
            assertThat(ignoreZValue, equalTo(true));
        }
        {
            // explicit false accept_z_value test
            DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
                b.field("type", "point");
                b.field("ignore_z_value", false);
            }));
            Mapper fieldMapper = mapper.mappers().getMapper(FIELD_NAME);
            assertThat(fieldMapper, instanceOf(PointFieldMapper.class));
            boolean ignoreZValue = ((PointFieldMapper) fieldMapper).ignoreZValue();
            assertThat(ignoreZValue, equalTo(false));
        }
    }

    public void testMultiFieldsDeprecationWarning() throws Exception {
        createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.startObject("fields");
            b.startObject("keyword").field("type", "keyword").endObject();
            b.endObject();
        }));
        assertWarnings("Adding multifields to [point] mappers has no effect and will be forbidden in future");
    }

    @Override
    protected List<ExampleMalformedValue> exampleMalformedValues() {
        return List.of(
            exampleMalformedValue("1234.333").errorMatches("expected 2 or 3 coordinates but found: [1]"),
            exampleMalformedValue(b -> b.startObject().field("x", 1.3).field("y", "-").endObject()).errorMatches(
                "[y] must be a valid double value"
            ),
            exampleMalformedValue(b -> b.startObject().field("x", "-").field("y", 1.3).endObject()).errorMatches(
                "[x] must be a valid double value"
            ),
            exampleMalformedValue(b -> b.startObject().field("geohash", stringEncode(0, 0)).endObject()).errorMatches(
                "field [geohash] not supported - must be one of: x, y, z, type, coordinates"
            ),
            exampleMalformedValue("-,1.3").errorMatches("[x] must be a number"),
            exampleMalformedValue("1.3,-").errorMatches("[y] must be a number"),
            exampleMalformedValue(b -> b.startObject().field("lon", 1.3).field("y", 1.3).endObject()).errorMatches(
                "field [lon] not supported - must be one of: x, y, z, type, coordinates"
            ),
            exampleMalformedValue(b -> b.startObject().field("x", 1.3).field("lat", 1.3).endObject()).errorMatches(
                "field [lat] not supported - must be one of: x, y, z, type, coordinates"
            ),
            exampleMalformedValue(b -> b.startObject().field("x", "NaN").field("y", "NaN").endObject()).errorMatches(
                "field must be either lat/lon or type/coordinates"
            ),
            exampleMalformedValue(b -> b.startObject().field("x", "NaN").field("y", 1.3).endObject()).errorMatches("Required [x]"),
            exampleMalformedValue(b -> b.startObject().field("x", 1.3).field("y", "NaN").endObject()).errorMatches("Required [y]"),
            exampleMalformedValue("NaN,NaN").errorMatches(
                "invalid [x] value [NaN]; must be between -3.4028234663852886E38 and 3.4028234663852886E38"
            ),
            exampleMalformedValue("10,NaN").errorMatches(
                "invalid [y] value [NaN]; must be between -3.4028234663852886E38 and 3.4028234663852886E38"
            ),
            exampleMalformedValue("NaN,12").errorMatches(
                "invalid [x] value [NaN]; must be between -3.4028234663852886E38 and 3.4028234663852886E38"
            ),
            exampleMalformedValue(b -> b.startObject().field("x", 1.3).nullField("y").endObject()).errorMatches("y must be a number"),
            exampleMalformedValue(b -> b.startObject().nullField("x").field("y", 1.3).endObject()).errorMatches("x must be a number")
        );
    }

    public void testGeoJSONInvalidType() throws IOException {
        double[] coords = new double[] { 0.0, 0.0 };
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Exception e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject(FIELD_NAME).field("coordinates", coords).field("type", "Polygon").endObject()))
        );
        assertThat(e.getMessage(), containsString("failed to parse"));
        assertThat(e.getCause().getMessage(), containsString("[type] for point can only be 'Point'"));
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("Test implemented in a follow up", true);
        return null;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected BlockReaderSupport getSupportedReaders(MapperService mapper, String loaderFieldName) {
        // TODO: Support testing both reading from source as well as reading from doc-values
        MappedFieldType ft = mapper.fieldType(loaderFieldName);
        PointFieldMapper.PointFieldType point = (PointFieldMapper.PointFieldType) ft;
        return new BlockReaderSupport(point.isIndexed() == false && ft.hasDocValues(), false, mapper, loaderFieldName);
    }
}
