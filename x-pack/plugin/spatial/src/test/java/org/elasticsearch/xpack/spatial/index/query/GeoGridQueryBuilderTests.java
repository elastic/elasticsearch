/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.query;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.h3.H3;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils.MAX_ZOOM;
import static org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils.longEncode;
import static org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils.stringEncode;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;

public class GeoGridQueryBuilderTests extends AbstractQueryTestCase<GeoGridQueryBuilder> {

    private static final String GEO_SHAPE_FIELD_NAME = "mapped_geo_shape";
    protected static final String GEO_SHAPE_ALIAS_FIELD_NAME = "mapped_geo_shape_alias";

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        final XContentBuilder builder = PutMappingRequest.simpleMapping(
            GEO_SHAPE_FIELD_NAME,
            "type=geo_shape",
            GEO_SHAPE_ALIAS_FIELD_NAME,
            "type=alias,path=" + GEO_SHAPE_FIELD_NAME
        );
        mapperService.merge("_doc", new CompressedXContent(Strings.toString(builder)), MapperService.MergeReason.MAPPING_UPDATE);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(LocalStateSpatialPlugin.class);
    }

    @Override
    protected GeoGridQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomFrom(GEO_POINT_FIELD_NAME, GEO_POINT_ALIAS_FIELD_NAME, GEO_SHAPE_FIELD_NAME, GEO_SHAPE_ALIAS_FIELD_NAME);
        GeoGridQueryBuilder builder = new GeoGridQueryBuilder(fieldName);

        // Only use geohex for points
        int path = randomIntBetween(0, GEO_SHAPE_FIELD_NAME.equals(fieldName) || GEO_SHAPE_ALIAS_FIELD_NAME.equals(fieldName) ? 1 : 3);
        switch (path) {
            case 0 -> builder.setGridId(GeoGridQueryBuilder.Grid.GEOHASH, randomGeohash());
            case 1 -> builder.setGridId(GeoGridQueryBuilder.Grid.GEOTILE, randomGeotile());
            default -> builder.setGridId(GeoGridQueryBuilder.Grid.GEOHEX, randomGeohex());
        }

        if (randomBoolean()) {
            builder.ignoreUnmapped(randomBoolean());
        }

        return builder;
    }

    private String randomGeohash() {
        return Geohash.stringEncode(GeometryTestUtils.randomLon(), GeometryTestUtils.randomLat(), randomIntBetween(1, Geohash.PRECISION));
    }

    private String randomGeotile() {
        final long encoded = GeoTileUtils.longEncode(
            GeometryTestUtils.randomLon(),
            GeometryTestUtils.randomLat(),
            randomIntBetween(0, GeoTileUtils.MAX_ZOOM)
        );
        return GeoTileUtils.stringEncode(encoded);
    }

    private String randomGeohex() {
        return H3.geoToH3Address(GeometryTestUtils.randomLat(), GeometryTestUtils.randomLon(), randomIntBetween(0, H3.MAX_H3_RES));
    }

    public void testValidationNullFieldname() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new GeoGridQueryBuilder((String) null));
        assertEquals("Field name must not be empty.", e.getMessage());
    }

    public void testExceptionOnMissingTypes() {
        SearchExecutionContext context = createShardContextWithNoType();
        GeoGridQueryBuilder qb = createTestQueryBuilder();
        qb.ignoreUnmapped(false);
        QueryShardException e = expectThrows(QueryShardException.class, () -> qb.toQuery(context));
        assertEquals("failed to find geo field [" + qb.fieldName() + "]", e.getMessage());
    }

    @Override
    protected void doAssertLuceneQuery(GeoGridQueryBuilder queryBuilder, Query query, SearchExecutionContext context) {
        final MappedFieldType fieldType = context.getFieldType(queryBuilder.fieldName());
        if (fieldType == null) {
            assertTrue("Found no indexed geo query.", query instanceof MatchNoDocsQuery);
        } else if (fieldType.hasDocValues()) {
            assertEquals(IndexOrDocValuesQuery.class, query.getClass());
        }
    }

    public void testParsingAndToQueryGeohex() throws IOException {
        String query = Strings.format("""
            {
                "geo_grid":{
                    "%s":{
                        "geohex": "%s"
                    }
                }
            }
            """, GEO_POINT_FIELD_NAME, randomGeohex());
        assertGeoGridQuery(query);
    }

    public void testParsingAndToQueryGeotile() throws IOException {
        String query = Strings.format("""
            {
                "geo_grid":{
                    "%s":{
                        "geotile": "%s"
                    }
                }
            }
            """, GEO_POINT_FIELD_NAME, randomGeotile());
        assertGeoGridQuery(query);
    }

    public void testParsingAndToQueryGeohash() throws IOException {
        String query = Strings.format("""
            {
                "geo_grid":{
                    "%s":{
                        "geohash": "%s"
                    }
                }
            }
            """, GEO_POINT_FIELD_NAME, randomGeohash());
        assertGeoGridQuery(query);
    }

    private void assertGeoGridQuery(String query) throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        // just check if we can parse the query
        parseQuery(query).toQuery(searchExecutionContext);
    }

    public void testMalformedGeoTile() {
        String query = Strings.format("""
            {
                "geo_grid":{
                    "%s":{
                        "geotile": "%s"
                    }
                }
            }
            """, GEO_POINT_FIELD_NAME, randomGeohex());
        IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, () -> parseQuery(query));
        assertThat(e1.getMessage(), containsString("Invalid geotile_grid hash string of"));
    }

    public void testMalformedGeohash() {
        String query = Strings.format("""
            {
                "geo_grid":{
                    "%s":{
                        "geohash": "%s"
                    }
                }
            }
            """, GEO_POINT_FIELD_NAME, randomGeotile());
        IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, () -> parseQuery(query));
        assertThat(e1.getMessage(), containsString("unsupported symbol [/] in geohash"));
    }

    public void testMalformedGeohex() {
        String query = Strings.format("""
            {
                "geo_grid":{
                    "%s":{
                        "geohex": "%s"
                    }
                }
            }
            """, GEO_POINT_FIELD_NAME, randomGeotile());
        IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, () -> parseQuery(query));
        assertThat(e1.getMessage(), containsString("Invalid h3 address"));
    }

    public void testWrongField() {
        String query = Strings.format("""
            {
                "geo_grid":{
                    "%s":{
                        "geohexes": "%s"
                    }
                }
            }
            """, GEO_POINT_FIELD_NAME, randomGeohex());
        ElasticsearchParseException e1 = expectThrows(ElasticsearchParseException.class, () -> parseQuery(query));
        assertThat(e1.getMessage(), containsString("Invalid grid name [geohexes]"));
    }

    public void testDuplicateField() {
        String query = Strings.format("""
            {
                "geo_grid":{
                    "%s":{
                        "geohex": "%s",
                        "geotile": "%s"
                    }
                }
            }
            """, GEO_POINT_FIELD_NAME, randomGeohex(), randomGeotile());
        ParsingException e1 = expectThrows(ParsingException.class, () -> parseQuery(query));
        assertThat(e1.getMessage(), containsString("failed to parse [geo_grid] query. unexpected field [geotile]"));
    }

    public void testIgnoreUnmapped() throws IOException {
        final GeoGridQueryBuilder queryBuilder = new GeoGridQueryBuilder("unmapped").setGridId(GeoGridQueryBuilder.Grid.GEOTILE, "0/0/0");
        queryBuilder.ignoreUnmapped(true);
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        Query query = queryBuilder.toQuery(searchExecutionContext);
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final GeoGridQueryBuilder failingQueryBuilder = new GeoGridQueryBuilder("unmapped").setGridId(
            GeoGridQueryBuilder.Grid.GEOTILE,
            "0/0/0"
        );
        failingQueryBuilder.ignoreUnmapped(false);
        QueryShardException e = expectThrows(QueryShardException.class, () -> failingQueryBuilder.toQuery(searchExecutionContext));
        assertThat(e.getMessage(), containsString("failed to find geo field [unmapped]"));
    }

    public void testGeohashBoundingBox() {
        double lat = randomDoubleBetween(-90d, 90d, true);
        double lon = randomDoubleBetween(-180d, 180d, true);
        double qLat = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(lat));
        double qLon = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lon));
        for (int zoom = 1; zoom <= Geohash.PRECISION; zoom++) {
            String hash = Geohash.stringEncode(qLon, qLat, zoom);
            Rectangle qRect = GeoGridQueryBuilder.getQueryHash(hash);
            assertBoundingBox(hash, zoom, qLon, qLat, qRect);
            assertBoundingBox(hash, zoom, qRect.getMinX(), qRect.getMinY(), qRect);
            assertBoundingBox(hash, zoom, qRect.getMaxX(), qRect.getMinY(), qRect);
            assertBoundingBox(hash, zoom, qRect.getMinX(), qRect.getMaxY(), qRect);
            assertBoundingBox(hash, zoom, qRect.getMaxX(), qRect.getMaxY(), qRect);
        }
    }

    private void assertBoundingBox(String hash, int precision, double lon, double lat, Rectangle r) {
        assertEquals(
            Geohash.stringEncode(lon, lat, precision).equals(hash),
            org.apache.lucene.geo.Rectangle.containsPoint(lat, lon, r.getMinLat(), r.getMaxLat(), r.getMinLon(), r.getMaxLon())
        );
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/92611")
    public void testBoundingBoxQuantize() {
        double lat = randomDoubleBetween(-GeoTileUtils.LATITUDE_MASK, GeoTileUtils.LATITUDE_MASK, true);
        double lon = randomDoubleBetween(-180d, 180d, true);
        double qLat = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(lat));
        double qLon = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lon));
        for (int zoom = 0; zoom < MAX_ZOOM; zoom++) {
            long tile = GeoTileUtils.longEncode(qLon, qLat, zoom);
            Rectangle qRect = GeoGridQueryBuilder.getQueryTile(stringEncode(tile));
            assertBoundingBox(tile, zoom, qLon, qLat, qRect);
            assertBoundingBox(tile, zoom, qRect.getMinX(), qRect.getMinY(), qRect);
            assertBoundingBox(tile, zoom, qRect.getMaxX(), qRect.getMinY(), qRect);
            assertBoundingBox(tile, zoom, qRect.getMinX(), qRect.getMaxY(), qRect);
            assertBoundingBox(tile, zoom, qRect.getMaxX(), qRect.getMaxY(), qRect);
        }
    }

    private void assertBoundingBox(long tile, int zoom, double lon, double lat, Rectangle r) {
        assertEquals(
            longEncode(lon, lat, zoom) == tile,
            org.apache.lucene.geo.Rectangle.containsPoint(lat, lon, r.getMinLat(), r.getMaxLat(), r.getMinLon(), r.getMaxLon())
        );
    }
}
