/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.sort;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NestedPathFieldMapper;
import org.elasticsearch.index.query.GeoValidationMethod;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.test.geo.RandomGeoGenerator;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.instanceOf;

public class GeoDistanceSortBuilderTests extends AbstractSortTestCase<GeoDistanceSortBuilder> {

    @Override
    protected GeoDistanceSortBuilder createTestItem() {
        return randomGeoDistanceSortBuilder();
    }

    public static GeoDistanceSortBuilder randomGeoDistanceSortBuilder() {
        String fieldName = randomAlphaOfLengthBetween(1, 10);
        GeoDistanceSortBuilder result = null;

        int id = randomIntBetween(0, 2);
        switch (id) {
            case 0 -> {
                int count = randomIntBetween(1, 10);
                String[] geohashes = new String[count];
                for (int i = 0; i < count; i++) {
                    geohashes[i] = RandomGeoGenerator.randomPoint(random()).geohash();
                }
                result = new GeoDistanceSortBuilder(fieldName, geohashes);
            }
            case 1 -> {
                GeoPoint pt = RandomGeoGenerator.randomPoint(random());
                result = new GeoDistanceSortBuilder(fieldName, pt.getLat(), pt.getLon());
            }
            case 2 -> result = new GeoDistanceSortBuilder(fieldName, points(new GeoPoint[0]));
            default -> throw new IllegalStateException("one of three geo initialisation strategies must be used");
        }
        if (randomBoolean()) {
            result.geoDistance(geoDistance(result.geoDistance()));
        }
        if (randomBoolean()) {
            result.unit(randomValueOtherThan(result.unit(), () -> randomFrom(DistanceUnit.values())));
        }
        if (randomBoolean()) {
            result.order(randomFrom(SortOrder.values()));
        }
        if (randomBoolean()) {
            result.sortMode(randomValueOtherThan(SortMode.SUM, () -> randomFrom(SortMode.values())));
        }
        if (randomBoolean()) {
            result.validation(randomValueOtherThan(result.validation(), () -> randomFrom(GeoValidationMethod.values())));
        }
        if (randomBoolean()) {
            NestedSortBuilder nestedSort = new NestedSortBuilder("path");
            nestedSort.setFilter(new MatchAllQueryBuilder());
            result.setNestedSort(nestedSort);
        }
        if (randomBoolean()) {
            result.ignoreUnmapped(result.ignoreUnmapped() == false);
        }
        return result;
    }

    @Override
    protected MappedFieldType provideMappedFieldType(String name) {
        return new GeoPointFieldMapper.GeoPointFieldType(name);
    }

    private static GeoPoint[] points(GeoPoint[] original) {
        GeoPoint[] result = null;
        while (result == null || Arrays.deepEquals(original, result)) {
            int count = randomIntBetween(1, 10);
            result = new GeoPoint[count];
            for (int i = 0; i < count; i++) {
                result[i] = RandomGeoGenerator.randomPoint(random());
            }
        }
        return result;
    }

    private static GeoDistance geoDistance(GeoDistance original) {
        int id = -1;
        while (id == -1 || (original != null && original.ordinal() == id)) {
            id = randomIntBetween(0, GeoDistance.values().length - 1);
        }
        return GeoDistance.values()[id];
    }

    @Override
    protected GeoDistanceSortBuilder mutate(GeoDistanceSortBuilder original) throws IOException {
        GeoDistanceSortBuilder result = new GeoDistanceSortBuilder(original);
        int parameter = randomIntBetween(0, 8);
        switch (parameter) {
            case 0:
                while (Arrays.deepEquals(original.points(), result.points())) {
                    GeoPoint pt = RandomGeoGenerator.randomPoint(random());
                    result.point(pt.getLat(), pt.getLon());
                }
                break;
            case 1:
                result.points(points(original.points()));
                break;
            case 2:
                result.geoDistance(geoDistance(original.geoDistance()));
                break;
            case 3:
                result.unit(randomValueOtherThan(result.unit(), () -> randomFrom(DistanceUnit.values())));
                break;
            case 4:
                result.order(randomValueOtherThan(original.order(), () -> randomFrom(SortOrder.values())));
                break;
            case 5:
                result.sortMode(
                    randomValueOtherThanMany(Arrays.asList(SortMode.SUM, result.sortMode())::contains, () -> randomFrom(SortMode.values()))
                );
                break;
            case 6:
                result.setNestedSort(
                    randomValueOtherThan(original.getNestedSort(), () -> NestedSortBuilderTests.createRandomNestedSort(3))
                );
                break;
            case 7:
                result.validation(randomValueOtherThan(result.validation(), () -> randomFrom(GeoValidationMethod.values())));
                break;
            case 8:
                result.ignoreUnmapped(result.ignoreUnmapped() == false);
                break;
        }
        return result;
    }

    @Override
    protected void sortFieldAssertions(GeoDistanceSortBuilder builder, SortField sortField, DocValueFormat format) throws IOException {
        assertEquals(builder.order() == SortOrder.ASC ? false : true, sortField.getReverse());
        assertEquals(builder.fieldName(), sortField.getField());
    }

    public void testSortModeSumIsRejectedInSetter() {
        GeoDistanceSortBuilder builder = new GeoDistanceSortBuilder("testname", -1, -1);
        GeoPoint point = RandomGeoGenerator.randomPoint(random());
        builder.point(point.getLat(), point.getLon());
        try {
            builder.sortMode(SortMode.SUM);
            fail("sort mode sum should not be supported");
        } catch (IllegalArgumentException e) {
            // all good
        }
    }

    public void testSortModeSumIsRejectedInJSON() throws IOException {
        String json = """
            {
              "testname" : [ {
                "lat" : -6.046997540714173,
                "lon" : -51.94128329747579
              } ],
              "unit" : "m",
              "distance_type" : "arc",
              "mode" : "SUM"
            }""";
        try (XContentParser itemParser = createParser(JsonXContent.jsonXContent, json)) {
            itemParser.nextToken();

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> GeoDistanceSortBuilder.fromXContent(itemParser, "")
            );
            assertEquals("sort_mode [sum] isn't supported for sorting by geo distance", e.getMessage());
        }
    }

    public void testGeoDistanceSortCanBeParsedFromGeoHash() throws IOException {
        String json = """
            {
                "VDcvDuFjE" : [ "7umzzv8eychg", "dmdgmt5z13uw",     "ezu09wxw6v4c", "kc7s3515p6k6", "jgeuvjwrmfzn", "kcpcfj7ruyf8" ],
                "unit" : "m",
                "distance_type" : "arc",
                "mode" : "MAX",
                "nested" : {
                  "filter" : {
                    "ids" : {
                      "values" : [ ],
                      "boost" : 5.711116
                    }
                  }
                },
                "validation_method" : "STRICT"
              }""";
        try (XContentParser itemParser = createParser(JsonXContent.jsonXContent, json)) {
            itemParser.nextToken();

            GeoDistanceSortBuilder result = GeoDistanceSortBuilder.fromXContent(itemParser, json);
            assertEquals(
                "[-19.700583312660456, -2.8225036337971687, "
                    + "31.537466906011105, -74.63590376079082, "
                    + "43.71844606474042, -5.548660643398762, "
                    + "-37.20467280596495, 38.71751043945551, "
                    + "-69.44606635719538, 84.25200328230858, "
                    + "-39.03717711567879, 44.74099852144718]",
                Arrays.toString(result.points())
            );
        }
    }

    public void testGeoDistanceSortParserManyPointsNoException() throws Exception {
        XContentBuilder sortBuilder = jsonBuilder();
        sortBuilder.startObject();
        sortBuilder.startArray("location");
        sortBuilder.startArray().value(1.2).value(3).endArray().startArray().value(5).value(6).endArray();
        sortBuilder.endArray();
        sortBuilder.field("order", "desc");
        sortBuilder.field("unit", "km");
        sortBuilder.field("mode", "max");
        sortBuilder.endObject();
        parse(sortBuilder);

        sortBuilder = jsonBuilder();
        sortBuilder.startObject();
        sortBuilder.startArray("location");
        sortBuilder.value(new GeoPoint(1.2, 3)).value(new GeoPoint(1.2, 3));
        sortBuilder.endArray();
        sortBuilder.field("order", "desc");
        sortBuilder.field("unit", "km");
        sortBuilder.field("mode", "max");
        sortBuilder.endObject();
        parse(sortBuilder);

        sortBuilder = jsonBuilder();
        sortBuilder.startObject();
        sortBuilder.startArray("location");
        sortBuilder.value("1,2").value("3,4");
        sortBuilder.endArray();
        sortBuilder.field("order", "desc");
        sortBuilder.field("unit", "km");
        sortBuilder.field("mode", "max");
        sortBuilder.endObject();
        parse(sortBuilder);

        sortBuilder = jsonBuilder();
        sortBuilder.startObject();
        sortBuilder.startArray("location");
        sortBuilder.value("s3y0zh7w1z0g").value("s6wjr4et3f8v");
        sortBuilder.endArray();
        sortBuilder.field("order", "desc");
        sortBuilder.field("unit", "km");
        sortBuilder.field("mode", "max");
        sortBuilder.endObject();
        parse(sortBuilder);

        sortBuilder = jsonBuilder();
        sortBuilder.startObject();
        sortBuilder.startArray("location");
        sortBuilder.value(1.2).value(3);
        sortBuilder.endArray();
        sortBuilder.field("order", "desc");
        sortBuilder.field("unit", "km");
        sortBuilder.field("mode", "max");
        sortBuilder.endObject();
        parse(sortBuilder);

        sortBuilder = jsonBuilder();
        sortBuilder.startObject();
        sortBuilder.field("location", new GeoPoint(1, 2));
        sortBuilder.field("order", "desc");
        sortBuilder.field("unit", "km");
        sortBuilder.field("mode", "max");
        sortBuilder.endObject();
        parse(sortBuilder);

        sortBuilder = jsonBuilder();
        sortBuilder.startObject();
        sortBuilder.field("location", "1,2");
        sortBuilder.field("order", "desc");
        sortBuilder.field("unit", "km");
        sortBuilder.field("mode", "max");
        sortBuilder.endObject();
        parse(sortBuilder);

        sortBuilder = jsonBuilder();
        sortBuilder.startObject();
        sortBuilder.field("location", "s3y0zh7w1z0g");
        sortBuilder.field("order", "desc");
        sortBuilder.field("unit", "km");
        sortBuilder.field("mode", "max");
        sortBuilder.endObject();
        parse(sortBuilder);

        sortBuilder = jsonBuilder();
        sortBuilder.startObject();
        sortBuilder.startArray("location");
        sortBuilder.value(new GeoPoint(1, 2)).value("s3y0zh7w1z0g").startArray().value(1).value(2).endArray().value("1,2");
        sortBuilder.endArray();
        sortBuilder.field("order", "desc");
        sortBuilder.field("unit", "km");
        sortBuilder.field("mode", "max");
        sortBuilder.endObject();
        parse(sortBuilder);
    }

    public void testGeoDistanceSortDeprecatedSortModeException() throws Exception {
        XContentBuilder sortBuilder = jsonBuilder();
        sortBuilder.startObject();
        sortBuilder.startArray("location");
        sortBuilder.startArray().value(1.2).value(3).endArray().startArray().value(5).value(6).endArray();
        sortBuilder.endArray();
        sortBuilder.field("order", "desc");
        sortBuilder.field("unit", "km");
        sortBuilder.field("sort_mode", "max");
        sortBuilder.endObject();
        parse(sortBuilder);
        assertWarnings("Deprecated field [sort_mode] used, expected [mode] instead");
    }

    private GeoDistanceSortBuilder parse(XContentBuilder sortBuilder) throws Exception {
        try (XContentParser parser = createParser(sortBuilder)) {
            parser.nextToken();
            return GeoDistanceSortBuilder.fromXContent(parser, null);
        }
    }

    @Override
    protected GeoDistanceSortBuilder fromXContent(XContentParser parser, String fieldName) throws IOException {
        return GeoDistanceSortBuilder.fromXContent(parser, fieldName);
    }

    public void testCommonCaseIsOptimized() throws IOException {
        // make sure the below tests test something...
        assertFalse(SortField.class.equals(LatLonDocValuesField.newDistanceSort("random_field_name", 3.5, 2.1).getClass()));

        SearchExecutionContext context = createMockSearchExecutionContext();
        // The common case should use LatLonDocValuesField.newDistanceSort
        GeoDistanceSortBuilder builder = new GeoDistanceSortBuilder("", new GeoPoint(3.5, 2.1));
        SortFieldAndFormat sort = builder.build(context);
        assertEquals(LatLonDocValuesField.newDistanceSort("random_field_name", 3.5, 2.1).getClass(), sort.field().getClass());

        // however this might be disabled by fancy options
        builder = new GeoDistanceSortBuilder("random_field_name", new GeoPoint(3.5, 2.1), new GeoPoint(3.0, 4));
        sort = builder.build(context);
        assertEquals(SortField.class, sort.field().getClass()); // 2 points -> plain SortField with a custom comparator

        builder = new GeoDistanceSortBuilder("random_field_name", new GeoPoint(3.5, 2.1));
        builder.unit(DistanceUnit.KILOMETERS);
        sort = builder.build(context);
        assertEquals(SortField.class, sort.field().getClass()); // km rather than m -> plain SortField with a custom comparator

        builder = new GeoDistanceSortBuilder("random_field_name", new GeoPoint(3.5, 2.1));
        builder.order(SortOrder.DESC);
        sort = builder.build(context);
        assertEquals(SortField.class, sort.field().getClass()); // descending means the max value should be considered rather than min

        builder = new GeoDistanceSortBuilder("random_field_name", new GeoPoint(3.5, 2.1));
        builder.setNestedSort(new NestedSortBuilder("path"));
        sort = builder.build(context);
        assertEquals(SortField.class, sort.field().getClass()); // can't use LatLon optimized sorting with nested fields

        builder = new GeoDistanceSortBuilder("random_field_name", new GeoPoint(3.5, 2.1));
        builder.order(SortOrder.DESC);
        sort = builder.build(context);
        assertEquals(SortField.class, sort.field().getClass()); // can't use LatLon optimized sorting with DESC sorting
    }

    /**
     * Test that the sort builder order gets transferred correctly to the SortField
     */
    public void testBuildSortFieldOrder() throws IOException {
        SearchExecutionContext searchExecutionContext = createMockSearchExecutionContext();
        GeoDistanceSortBuilder geoDistanceSortBuilder = new GeoDistanceSortBuilder("fieldName", 1.0, 1.0);
        assertEquals(false, geoDistanceSortBuilder.build(searchExecutionContext).field().getReverse());

        geoDistanceSortBuilder.order(SortOrder.ASC);
        assertEquals(false, geoDistanceSortBuilder.build(searchExecutionContext).field().getReverse());

        geoDistanceSortBuilder.order(SortOrder.DESC);
        assertEquals(true, geoDistanceSortBuilder.build(searchExecutionContext).field().getReverse());
    }

    /**
     * Test that the sort builder mode gets transferred correctly to the SortField
     */
    public void testMultiValueMode() throws IOException {
        SearchExecutionContext searchExecutionContext = createMockSearchExecutionContext();
        GeoDistanceSortBuilder geoDistanceSortBuilder = new GeoDistanceSortBuilder("fieldName", 1.0, 1.0);
        geoDistanceSortBuilder.sortMode(SortMode.MAX);
        SortField sortField = geoDistanceSortBuilder.build(searchExecutionContext).field();
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        XFieldComparatorSource comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        assertEquals(MultiValueMode.MAX, comparatorSource.sortMode());

        // also use MultiValueMode.Max if no Mode set but order is DESC
        geoDistanceSortBuilder = new GeoDistanceSortBuilder("fieldName", 1.0, 1.0);
        geoDistanceSortBuilder.order(SortOrder.DESC);
        sortField = geoDistanceSortBuilder.build(searchExecutionContext).field();
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        assertEquals(MultiValueMode.MAX, comparatorSource.sortMode());

        // use MultiValueMode.Min if no Mode and order is ASC
        geoDistanceSortBuilder = new GeoDistanceSortBuilder("fieldName", 1.0, 1.0);
        // need to use distance unit other than Meters to not get back a LatLonPointSortField
        geoDistanceSortBuilder.order(SortOrder.ASC).unit(DistanceUnit.INCH);
        sortField = geoDistanceSortBuilder.build(searchExecutionContext).field();
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        assertEquals(MultiValueMode.MIN, comparatorSource.sortMode());

        geoDistanceSortBuilder = new GeoDistanceSortBuilder("fieldName", 1.0, 1.0);
        // need to use distance unit other than Meters to not get back a LatLonPointSortField
        geoDistanceSortBuilder.sortMode(SortMode.MIN).unit(DistanceUnit.INCH);
        sortField = geoDistanceSortBuilder.build(searchExecutionContext).field();
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        assertEquals(MultiValueMode.MIN, comparatorSource.sortMode());

        geoDistanceSortBuilder.sortMode(SortMode.AVG);
        sortField = geoDistanceSortBuilder.build(searchExecutionContext).field();
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        assertEquals(MultiValueMode.AVG, comparatorSource.sortMode());

        geoDistanceSortBuilder.sortMode(SortMode.MEDIAN);
        sortField = geoDistanceSortBuilder.build(searchExecutionContext).field();
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        assertEquals(MultiValueMode.MEDIAN, comparatorSource.sortMode());
    }

    /**
     * Test that the sort builder nested object gets created in the SortField
     */
    public void testBuildNested() throws IOException {
        SearchExecutionContext searchExecutionContext = createMockSearchExecutionContext();

        GeoDistanceSortBuilder sortBuilder = new GeoDistanceSortBuilder("fieldName", 1.0, 1.0).setNestedSort(
            new NestedSortBuilder("path").setFilter(QueryBuilders.matchAllQuery())
        );
        SortField sortField = sortBuilder.build(searchExecutionContext).field();
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        XFieldComparatorSource comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        Nested nested = comparatorSource.nested();
        assertNotNull(nested);
        assertEquals(new MatchAllDocsQuery(), nested.getInnerQuery());

        sortBuilder = new GeoDistanceSortBuilder("fieldName", 1.0, 1.0).setNestedSort(new NestedSortBuilder("path"));
        sortField = sortBuilder.build(searchExecutionContext).field();
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        nested = comparatorSource.nested();
        assertNotNull(nested);
        assertEquals(new TermQuery(new Term(NestedPathFieldMapper.NAME, "path")), nested.getInnerQuery());

        sortBuilder = new GeoDistanceSortBuilder("fieldName", 1.0, 1.0).setNestedSort(
            new NestedSortBuilder("path").setFilter(QueryBuilders.matchAllQuery())
        );
        sortField = sortBuilder.build(searchExecutionContext).field();
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        nested = comparatorSource.nested();
        assertNotNull(nested);
        assertEquals(new MatchAllDocsQuery(), nested.getInnerQuery());
    }

    /**
     * Test that if coercion is used, a point gets normalized but the original values in the builder are unchanged
     */
    public void testBuildCoerce() throws IOException {
        SearchExecutionContext searchExecutionContext = createMockSearchExecutionContext();
        GeoDistanceSortBuilder sortBuilder = new GeoDistanceSortBuilder("fieldName", -180.0, -360.0);
        sortBuilder.validation(GeoValidationMethod.COERCE);
        assertEquals(-180.0, sortBuilder.points()[0].getLat(), 0.0);
        assertEquals(-360.0, sortBuilder.points()[0].getLon(), 0.0);
        SortField sortField = sortBuilder.build(searchExecutionContext).field();
        assertEquals(LatLonDocValuesField.newDistanceSort("fieldName", 0.0, 180.0), sortField);
    }

    /**
     * Test that if validation is strict, invalid points throw an error
     */
    public void testBuildInvalidPoints() throws IOException {
        SearchExecutionContext searchExecutionContext = createMockSearchExecutionContext();
        {
            GeoDistanceSortBuilder sortBuilder = new GeoDistanceSortBuilder("fieldName", -180.0, 0.0);
            sortBuilder.validation(GeoValidationMethod.STRICT);
            ElasticsearchParseException ex = expectThrows(
                ElasticsearchParseException.class,
                () -> sortBuilder.build(searchExecutionContext)
            );
            assertEquals("illegal latitude value [-180.0] for [GeoDistanceSort] for field [fieldName].", ex.getMessage());
        }
        {
            GeoDistanceSortBuilder sortBuilder = new GeoDistanceSortBuilder("fieldName", 0.0, -360.0);
            sortBuilder.validation(GeoValidationMethod.STRICT);
            ElasticsearchParseException ex = expectThrows(
                ElasticsearchParseException.class,
                () -> sortBuilder.build(searchExecutionContext)
            );
            assertEquals("illegal longitude value [-360.0] for [GeoDistanceSort] for field [fieldName].", ex.getMessage());
        }
    }

    /**
     * Test the nested Filter gets rewritten
     */
    public void testNestedRewrites() throws IOException {
        GeoDistanceSortBuilder sortBuilder = new GeoDistanceSortBuilder("fieldName", 0.0, 0.0);
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder("fieldName") {
            @Override
            public QueryBuilder doSearchRewrite(SearchExecutionContext context) {
                return new MatchNoneQueryBuilder();
            }
        };
        sortBuilder.setNestedSort(new NestedSortBuilder("path").setFilter(rangeQuery));
        GeoDistanceSortBuilder rewritten = sortBuilder.rewrite(createMockSearchExecutionContext());
        assertNotSame(rangeQuery, rewritten.getNestedSort().getFilter());
    }

    /**
     * Test the nested sort gets rewritten
     */
    public void testNestedSortRewrites() throws IOException {
        GeoDistanceSortBuilder sortBuilder = new GeoDistanceSortBuilder("fieldName", 0.0, 0.0);
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder("fieldName") {
            @Override
            public QueryBuilder doSearchRewrite(SearchExecutionContext context) {
                return new MatchNoneQueryBuilder();
            }
        };
        sortBuilder.setNestedSort(new NestedSortBuilder("path").setFilter(rangeQuery));
        GeoDistanceSortBuilder rewritten = sortBuilder.rewrite(createMockSearchExecutionContext());
        assertNotSame(rangeQuery, rewritten.getNestedSort().getFilter());
    }

}
