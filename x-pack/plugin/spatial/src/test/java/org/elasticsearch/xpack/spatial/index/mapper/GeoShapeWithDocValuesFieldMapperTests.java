/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.AbstractGeometryFieldMapper;
import org.elasticsearch.index.mapper.AbstractShapeGeometryFieldMapper;
import org.elasticsearch.index.mapper.AbstractShapeGeometryFieldMapper.AbstractShapeGeometryFieldType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class GeoShapeWithDocValuesFieldMapperTests extends GeoFieldMapperTests {

    @Override
    protected String getFieldName() {
        return "geo_shape";
    }

    @Override
    protected boolean supportsSearchLookup() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("store", b -> b.field("store", true));
        checker.registerUpdateCheck(b -> b.field("orientation", "right"), m -> {
            AbstractShapeGeometryFieldMapper<?> gsfm = (AbstractShapeGeometryFieldMapper<?>) m;
            assertEquals(Orientation.RIGHT, gsfm.orientation());
        });
        checker.registerUpdateCheck(b -> b.field("ignore_z_value", false), m -> {
            AbstractShapeGeometryFieldMapper<?> gpfm = (AbstractShapeGeometryFieldMapper<?>) m;
            assertFalse(gpfm.ignoreZValue());
        });
        checker.registerUpdateCheck(b -> b.field("coerce", true), m -> {
            AbstractShapeGeometryFieldMapper<?> gpfm = (AbstractShapeGeometryFieldMapper<?>) m;
            assertTrue(gpfm.coerce());
        });
    }

    protected AbstractShapeGeometryFieldType<?> fieldType(Mapper fieldMapper) {
        AbstractShapeGeometryFieldMapper<?> shapeFieldMapper = (AbstractShapeGeometryFieldMapper<?>) fieldMapper;
        return (AbstractShapeGeometryFieldType<?>) shapeFieldMapper.fieldType();
    }

    protected Class<? extends AbstractShapeGeometryFieldMapper<?>> fieldMapperClass() {
        return GeoShapeWithDocValuesFieldMapper.class;
    }

    public void testDefaultConfiguration() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Mapper fieldMapper = mapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(fieldMapperClass()));

        AbstractShapeGeometryFieldType<?> fieldType = fieldType(fieldMapper);
        assertThat(fieldType.orientation(), equalTo(Orientation.RIGHT));
        assertTrue(fieldType.hasDocValues());
    }

    public void testDefaultDocValueConfigurationOnPre7_8() throws IOException {
        IndexVersion oldVersion = IndexVersionUtils.randomVersionBetween(random(), IndexVersions.V_7_0_0, IndexVersions.V_7_7_0);
        DocumentMapper defaultMapper = createDocumentMapper(oldVersion, fieldMapping(this::minimalMapping));
        Mapper fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(fieldMapperClass()));

        GeoShapeWithDocValuesFieldMapper geoShapeFieldMapper = (GeoShapeWithDocValuesFieldMapper) fieldMapper;
        assertFalse(geoShapeFieldMapper.fieldType().hasDocValues());
    }

    /**
     * Test that orientation parameter correctly parses
     */
    public void testOrientationParsing() throws IOException {

        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("orientation", "left");
        }));
        Mapper fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(fieldMapperClass()));

        AbstractShapeGeometryFieldType<?> fieldType = fieldType(fieldMapper);
        Orientation orientation = fieldType.orientation();
        assertThat(orientation, equalTo(Orientation.CLOCKWISE));
        assertThat(orientation, equalTo(Orientation.LEFT));
        assertThat(orientation, equalTo(Orientation.CW));

        // explicit right orientation test
        defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("orientation", "right");
        }));
        fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(fieldMapperClass()));

        fieldType = fieldType(fieldMapper);
        orientation = fieldType.orientation();
        assertThat(orientation, equalTo(Orientation.COUNTER_CLOCKWISE));
        assertThat(orientation, equalTo(Orientation.RIGHT));
        assertThat(orientation, equalTo(Orientation.CCW));
    }

    /**
     * Test that coerce parameter correctly parses
     */
    public void testCoerceParsing() throws IOException {

        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("coerce", true);
        }));
        Mapper fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(fieldMapperClass()));

        boolean coerce = ((AbstractShapeGeometryFieldMapper<?>) fieldMapper).coerce();
        assertThat(coerce, equalTo(true));

        defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("coerce", false);
        }));
        fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(fieldMapperClass()));

        coerce = ((AbstractShapeGeometryFieldMapper<?>) fieldMapper).coerce();
        assertThat(coerce, equalTo(false));

    }

    /**
     * Test that accept_z_value parameter correctly parses
     */
    public void testIgnoreZValue() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("ignore_z_value", true);
        }));
        Mapper fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(fieldMapperClass()));

        boolean ignoreZValue = ((AbstractGeometryFieldMapper<?>) fieldMapper).ignoreZValue();
        assertThat(ignoreZValue, equalTo(true));

        // explicit false accept_z_value test
        defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("ignore_z_value", false);
        }));
        fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(fieldMapperClass()));

        ignoreZValue = ((AbstractGeometryFieldMapper<?>) fieldMapper).ignoreZValue();
        assertThat(ignoreZValue, equalTo(false));
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return true;
    }

    @Override
    protected List<ExampleMalformedValue> exampleMalformedValues() {
        return List.of(
            exampleMalformedValue("Bad shape").errorMatches("Unknown geometry type: bad"),
            exampleMalformedValue(
                "POLYGON ((18.9401790919516 -33.9681188869036, 18.9401790919516 -33.9681188869036, 18.9401790919517 "
                    + "-33.9681188869036, 18.9401790919517 -33.9681188869036, 18.9401790919516 -33.9681188869036))"
            ).errorMatches("at least three non-collinear points required")
        );
    }

    /**
     * Test that doc_values parameter correctly parses
     */
    public void testDocValues() throws IOException {

        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("doc_values", true);
        }));
        Mapper fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(fieldMapperClass()));

        boolean hasDocValues = ((AbstractGeometryFieldMapper<?>) fieldMapper).fieldType().hasDocValues();
        assertTrue(hasDocValues);
        assertTrue(((AbstractGeometryFieldMapper<?>) fieldMapper).fieldType().isAggregatable());

        // explicit false doc_values
        defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("doc_values", false);
        }));
        fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(fieldMapperClass()));

        hasDocValues = ((AbstractGeometryFieldMapper<?>) fieldMapper).fieldType().hasDocValues();
        assertFalse(hasDocValues);
        assertFalse(((AbstractGeometryFieldMapper<?>) fieldMapper).fieldType().isAggregatable());
    }

    /**
     * Test that store parameter correctly parses
     */
    public void testStore() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("store", true);
        }));
        Mapper fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(fieldMapperClass()));

        boolean isStored = ((AbstractGeometryFieldMapper<?>) fieldMapper).fieldType().isStored();
        assertTrue(isStored);

        // explicit false doc_values
        defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("store", false);
        }));
        fieldMapper = defaultMapper.mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(fieldMapperClass()));

        isStored = ((AbstractGeometryFieldMapper<?>) fieldMapper).fieldType().isStored();
        assertFalse(isStored);
    }

    public void testShapeMapperMerge() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("orientation", "ccw");
        }));

        merge(mapperService, fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("orientation", "cw");
        }));

        Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper(FIELD_NAME);
        assertThat(fieldMapper, instanceOf(fieldMapperClass()));

        AbstractShapeGeometryFieldType<?> fieldType = fieldType(fieldMapper);
        assertThat(fieldType.orientation(), equalTo(Orientation.CW));
    }

    public void testInvalidCurrentVersion() {
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> super.createMapperService(IndexVersion.current(), fieldMapping((b) -> {
                b.field("type", getFieldName()).field("strategy", "recursive");
            }))
        );
        assertThat(
            e.getMessage(),
            containsString("using deprecated parameters [strategy] " + "in mapper [field] of type [geo_shape] is no longer allowed")
        );
    }

    public void testGeoShapeLegacyMerge() throws Exception {
        IndexVersion version = IndexVersionUtils.randomPreviousCompatibleVersion(random(), IndexVersions.V_8_0_0);
        MapperService m = createMapperService(version, fieldMapping(b -> b.field("type", getFieldName())));
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> merge(m, fieldMapping(b -> b.field("type", getFieldName()).field("strategy", "recursive")))
        );

        assertThat(e.getMessage(), containsString("mapper [field] of type [geo_shape] cannot change strategy from [BKD] to [recursive]"));
        assertFieldWarnings("strategy");

        MapperService lm = createMapperService(version, fieldMapping(b -> b.field("type", getFieldName()).field("strategy", "recursive")));
        e = expectThrows(IllegalArgumentException.class, () -> merge(lm, fieldMapping(b -> b.field("type", getFieldName()))));
        assertThat(e.getMessage(), containsString("mapper [field] of type [geo_shape] cannot change strategy from [recursive] to [BKD]"));
        assertFieldWarnings("strategy");
    }

    private void assertFieldWarnings(String... fieldNames) {
        String[] warnings = new String[fieldNames.length];
        for (int i = 0; i < fieldNames.length; ++i) {
            warnings[i] = "Parameter [" + fieldNames[i] + "] " + "is deprecated and will be removed in a future version";
        }
        assertWarnings(warnings);
    }

    public void testSerializeDefaults() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        String serialized = toXContentString((GeoShapeWithDocValuesFieldMapper) defaultMapper.mappers().getMapper(FIELD_NAME));
        assertTrue(serialized, serialized.contains("\"orientation\":\"" + Orientation.RIGHT + "\""));
        assertTrue(serialized, serialized.contains("\"doc_values\":true"));
        assertTrue(serialized, serialized.contains("\"store\":false"));
        assertTrue(serialized, serialized.contains("\"index\":true"));
    }

    public void testSerializeNonDefaultsDefaults() throws Exception {
        boolean docValues = randomBoolean();
        boolean store = randomBoolean();
        boolean index = randomBoolean();
        Orientation orientation = randomBoolean() ? Orientation.LEFT : Orientation.RIGHT;
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("doc_values", docValues);
            b.field("store", store);
            b.field("index", index);
            b.field("orientation", orientation);
        }));
        String serialized = toXContentString((GeoShapeWithDocValuesFieldMapper) mapper.mappers().getMapper(FIELD_NAME));
        assertTrue(serialized, serialized.contains("\"orientation\":\"" + orientation + "\""));
        assertTrue(serialized, serialized.contains("\"doc_values\":" + docValues));
        assertTrue(serialized, serialized.contains("\"store\":" + store));
        assertTrue(serialized, serialized.contains("\"index\":" + index));
    }

    public void testSerializeDocValues() throws IOException {
        boolean docValues = randomBoolean();
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("doc_values", docValues);
        }));
        String serialized = toXContentString((GeoShapeWithDocValuesFieldMapper) mapper.mappers().getMapper(FIELD_NAME));
        assertTrue(serialized, serialized.contains("\"orientation\":\"" + Orientation.RIGHT + "\""));
        assertTrue(serialized, serialized.contains("\"doc_values\":" + docValues));
        assertTrue(serialized, serialized.contains("\"store\":false"));
        assertTrue(serialized, serialized.contains("\"index\":true"));
    }

    public void testShapeArrayParsing() throws Exception {

        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("store", true);
        }));

        SourceToParse sourceToParse = source(b -> {
            b.startArray(FIELD_NAME)
                .startObject()
                .field("type", "Point")
                .startArray("coordinates")
                .value(176.0)
                .value(15.0)
                .endArray()
                .endObject()
                .startObject()
                .field("type", "Point")
                .startArray("coordinates")
                .value(76.0)
                .value(-15.0)
                .endArray()
                .endObject()
                .endArray();
        });

        ParsedDocument document = mapper.parse(sourceToParse);
        assertThat(document.docs(), hasSize(1));
        List<IndexableField> fields = document.docs().get(0).getFields(FIELD_NAME);
        // 2 BKD points, 2 stored fields and 1 doc value
        assertThat(fields, hasSize(5));
    }

    public void testMultiFieldsDeprecationWarning() throws Exception {
        createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.startObject("fields");
            b.startObject("keyword").field("type", "keyword").endObject();
            b.endObject();
        }));
        assertWarnings("Adding multifields to [" + getFieldName() + "] mappers has no effect and will be forbidden in future");
    }

    public void testSelfIntersectPolygon() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        DocumentParsingException ex = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.field("field", "POLYGON((0 0, 1 1, 0 1, 1 0, 0 0))")))
        );
        assertThat(ex.getCause().getMessage(), containsString("Polygon self-intersection at lat=0.5 lon=0.5"));
    }

    public String toXContentString(AbstractShapeGeometryFieldMapper<?> mapper, boolean includeDefaults) {
        if (includeDefaults) {
            ToXContent.Params params = new ToXContent.MapParams(Collections.singletonMap("include_defaults", "true"));
            return Strings.toString(mapper, params);
        } else {
            return Strings.toString(mapper);
        }
    }

    public String toXContentString(AbstractShapeGeometryFieldMapper<?> mapper) {
        return toXContentString(mapper, true);
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("Test implemented in a follow up", true);
        return null;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        // Almost like GeoShapeType but no circles
        enum ShapeType {
            POINT,
            LINESTRING,
            POLYGON,
            MULTIPOINT,
            MULTILINESTRING,
            MULTIPOLYGON,
            GEOMETRYCOLLECTION,
            ENVELOPE
        }

        return new SyntheticSourceSupport() {
            @Override
            public boolean preservesExactSource() {
                return true;
            }

            @Override
            public SyntheticSourceExample example(int maxValues) throws IOException {
                if (randomBoolean()) {
                    Value v = generateValue();
                    if (v.blockLoaderOutput != null) {
                        return new SyntheticSourceExample(v.input, v.output, v.blockLoaderOutput, this::mapping);
                    }
                    return new SyntheticSourceExample(v.input, v.output, this::mapping);
                }

                List<Value> values = randomList(1, maxValues, this::generateValue);
                List<Object> in = values.stream().map(Value::input).toList();
                List<Object> out = values.stream().map(Value::output).toList();

                // Block loader infrastructure will never return nulls
                List<Object> outBlockList = values.stream()
                    .filter(v -> v.input != null)
                    .map(v -> v.blockLoaderOutput != null ? v.blockLoaderOutput : v.output)
                    .toList();
                var outBlock = outBlockList.size() == 1 ? outBlockList.get(0) : outBlockList;

                return new SyntheticSourceExample(in, out, outBlock, this::mapping);
            }

            private record Value(Object input, Object output, String blockLoaderOutput) {
                Value(Object input, Object output) {
                    this(input, output, null);
                }
            }

            private Value generateValue() {
                if (ignoreMalformed && randomBoolean()) {
                    List<Supplier<Object>> choices = List.of(
                        () -> randomAlphaOfLength(3),
                        ESTestCase::randomInt,
                        ESTestCase::randomLong,
                        ESTestCase::randomFloat,
                        ESTestCase::randomDouble
                    );
                    Object v = randomFrom(choices).get();
                    return new Value(v, v);
                }
                if (randomBoolean()) {
                    return new Value(null, null);
                }

                var type = randomFrom(ShapeType.values());
                var isGeoJson = randomBoolean();

                switch (type) {
                    case POINT -> {
                        var point = GeometryTestUtils.randomPoint(false);
                        return value(point, isGeoJson);
                    }
                    case LINESTRING -> {
                        var line = GeometryTestUtils.randomLine(false);
                        return value(line, isGeoJson);
                    }
                    case POLYGON -> {
                        var polygon = GeometryTestUtils.randomPolygon(false);
                        return value(polygon, isGeoJson);
                    }
                    case MULTIPOINT -> {
                        var multiPoint = GeometryTestUtils.randomMultiPoint(false);
                        return value(multiPoint, isGeoJson);
                    }
                    case MULTILINESTRING -> {
                        var multiPoint = GeometryTestUtils.randomMultiLine(false);
                        return value(multiPoint, isGeoJson);
                    }
                    case MULTIPOLYGON -> {
                        var multiPolygon = GeometryTestUtils.randomMultiPolygon(false);
                        return value(multiPolygon, isGeoJson);
                    }
                    case GEOMETRYCOLLECTION -> {
                        var multiPolygon = GeometryTestUtils.randomGeometryCollectionWithoutCircle(false);
                        return value(multiPolygon, isGeoJson);
                    }
                    case ENVELOPE -> {
                        var rectangle = GeometryTestUtils.randomRectangle();
                        var wktString = WellKnownText.toWKT(rectangle);

                        return new Value(wktString, wktString);
                    }
                    default -> throw new UnsupportedOperationException("Unsupported shape");
                }
            }

            private static Value value(Geometry geometry, boolean isGeoJson) {
                var wktString = WellKnownText.toWKT(geometry);
                var normalizedWktString = GeometryNormalizer.needsNormalize(Orientation.RIGHT, geometry)
                    ? WellKnownText.toWKT(GeometryNormalizer.apply(Orientation.RIGHT, geometry))
                    : wktString;

                if (isGeoJson) {
                    var map = GeoJson.toMap(geometry);
                    return new Value(map, map, normalizedWktString);
                }

                return new Value(wktString, wktString, normalizedWktString);
            }

            private void mapping(XContentBuilder b) throws IOException {
                b.field("type", "geo_shape");
                if (rarely()) {
                    b.field("index", false);
                }
                if (rarely()) {
                    b.field("doc_values", false);
                }
                if (ignoreMalformed) {
                    b.field("ignore_malformed", true);
                }
            }

            @Override
            public List<SyntheticSourceInvalidExample> invalidExample() throws IOException {
                return List.of();
            }
        };
    }

    @Override
    protected Function<Object, Object> loadBlockExpected(BlockReaderSupport blockReaderSupport, boolean columnReader) {
        return v -> asWKT((BytesRef) v);
    }

    protected static Object asWKT(BytesRef value) {
        // Internally we use WKB in BytesRef, but for test assertions we want to use WKT for readability
        Geometry geometry = WellKnownBinary.fromWKB(GeometryValidator.NOOP, false, value.bytes);
        return WellKnownText.toWKT(geometry);
    }

    @Override
    protected BlockReaderSupport getSupportedReaders(MapperService mapper, String loaderFieldName) {
        // Synthetic source is currently not supported.
        return new BlockReaderSupport(false, false, mapper, loaderFieldName);
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }
}
