/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.AbstractGeometryFieldMapper;
import org.elasticsearch.index.mapper.AbstractShapeGeometryFieldMapper;
import org.elasticsearch.index.mapper.AbstractShapeGeometryFieldMapper.AbstractShapeGeometryFieldType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.legacygeo.mapper.LegacyGeoShapeFieldMapper;
import org.elasticsearch.legacygeo.mapper.LegacyGeoShapeFieldMapper.GeoShapeFieldType;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.ToXContent.MapParams;
import org.elasticsearch.xcontent.ToXContent.Params;
import org.elasticsearch.xpack.spatial.index.mapper.GeometricShapeSyntheticSourceSupport.FieldType;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.legacygeo.mapper.LegacyGeoShapeFieldMapper.DEPRECATED_PARAMETERS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    /**
     * Test that we can parse legacy v7 "geo_shape" parameters for BWC with read-only N-2 indices
     */
    public void testGeoShapeLegacyParametersParsing() throws Exception {
        // deprecated parameters needed for bwc with read-only version 7 indices
        assertEquals(Set.of("strategy", "tree", "tree_levels", "precision", "distance_error_pct", "points_only"), DEPRECATED_PARAMETERS);

        for (String deprecatedParam : DEPRECATED_PARAMETERS) {
            Object value = switch (deprecatedParam) {
                case "tree" -> randomFrom("quadtree", "geohash");
                case "tree_levels" -> 6;
                case "distance_error_pct" -> "0.01";
                case "points_only" -> true;
                case "strategy" -> "recursive";
                case "precision" -> "50m";
                default -> throw new IllegalStateException("Unexpected value: " + deprecatedParam);
            };

            // indices created before 8 should allow parameters but issue a warning
            IndexVersion pre8version = IndexVersionUtils.randomPreviousCompatibleVersion(random(), IndexVersions.V_8_0_0);
            MapperService m = createMapperService(
                pre8version,
                fieldMapping(b -> b.field("type", getFieldName()).field(deprecatedParam, value))
            );

            // check mapper
            Mapper mapper = m.mappingLookup().getMapper("field");
            assertThat(mapper, instanceOf(LegacyGeoShapeFieldMapper.class));

            // check document parsing
            MultiPoint multiPoint = GeometryTestUtils.randomMultiPoint(false);
            assertNotNull(m.documentMapper().parse(source(b -> {
                b.field("field");
                GeoJson.toXContent(multiPoint, b, null);
            })));

            // check for correct field type and that a query can be created
            MappedFieldType fieldType = m.fieldType("field");
            assertThat(fieldType, instanceOf(GeoShapeFieldType.class));
            GeoShapeFieldType ft = (GeoShapeFieldType) fieldType;

            SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
            when(searchExecutionContext.allowExpensiveQueries()).thenReturn(true);
            assertNotNull(
                ft.geoShapeQuery(searchExecutionContext, "location", SpatialStrategy.TERM, ShapeRelation.INTERSECTS, new Point(-10, 10))
            );
            if (deprecatedParam.equals("strategy") == false) {
                assertFieldWarnings(deprecatedParam, "strategy");
            } else {
                assertFieldWarnings(deprecatedParam);
            }

            // indices created after 8 should throw an error
            IndexVersion post8version = IndexVersionUtils.randomCompatibleWriteVersion(random());
            Exception ex = expectThrows(
                MapperParsingException.class,
                () -> createMapperService(post8version, fieldMapping(b -> b.field("type", getFieldName()).field(deprecatedParam, value)))
            );
            assertThat(
                ex.getMessage(),
                containsString(
                    "Failed to parse mapping: using deprecated parameters ["
                        + deprecatedParam
                        + "] in mapper [field] of type [geo_shape] is no longer allowed"
                )
            );
        }
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

    public void testGeoShapeLegacyCircle() throws Exception {
        IndexVersion version = IndexVersionUtils.randomPreviousCompatibleVersion(random(), IndexVersions.V_8_0_0);
        MapperService mapperService = createMapperService(version, fieldMapping(b -> {
            b.field("type", getFieldName());
            b.field("strategy", "recursive");
            b.field("tree", "geohash");
        }));
        assertCriticalWarnings(
            "Parameter [strategy] is deprecated and will be removed in a future version",
            "Parameter [tree] is deprecated and will be removed in a future version"
        );

        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            Circle circle = new Circle(30, 50, 77000);

            LuceneDocument doc = mapperService.documentMapper().parse(source(b -> {
                b.field("field");
                GeoJson.toXContent(circle, b, null);
            })).rootDoc();
            iw.addDocument(doc);
            iw.close();
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                SearchExecutionContext context = createSearchExecutionContext(mapperService, newSearcher(reader));
                GeoShapeQueryBuilder queryBuilder = new GeoShapeQueryBuilder("field", new Circle(30, 50, 77000));
                TopDocs docs = context.searcher().search(queryBuilder.toQuery(context), 1);
                assertThat(docs.totalHits.value(), equalTo(1L));
                assertThat(docs.totalHits.relation(), equalTo(TotalHits.Relation.EQUAL_TO));
            }
        }
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
        assertWarnings("Adding multifields to [" + getFieldName() + "] mappers has no effect");
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
            Params params = new MapParams(Collections.singletonMap("include_defaults", "true"));
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
        return new GeometricShapeSyntheticSourceSupport(FieldType.GEO_SHAPE, ignoreMalformed);
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }
}
