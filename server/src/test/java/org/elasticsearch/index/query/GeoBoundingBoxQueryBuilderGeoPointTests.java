/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.geo.GeoBoundingBoxQueryBuilderTestCase;

import java.io.IOException;

@SuppressWarnings("checkstyle:MissingJavadocMethod")
public class GeoBoundingBoxQueryBuilderGeoPointTests extends GeoBoundingBoxQueryBuilderTestCase {

    @Override
    protected String getFieldName() {
        return randomFrom(GEO_POINT_FIELD_NAME, GEO_POINT_ALIAS_FIELD_NAME);
    }

    @Override
    protected void doAssertLuceneQuery(GeoBoundingBoxQueryBuilder queryBuilder, Query query, SearchExecutionContext context)
        throws IOException {
        final MappedFieldType fieldType = context.getFieldType(queryBuilder.fieldName());
        if (fieldType == null) {
            assertTrue("Found no indexed geo query.", query instanceof MatchNoDocsQuery);
        }
        assertEquals(GeoPointFieldMapper.GeoPointFieldType.class, fieldType.getClass());
        assertEquals(IndexOrDocValuesQuery.class, query.getClass());
        Query indexQuery = ((IndexOrDocValuesQuery) query).getIndexQuery();
        String expectedFieldName = expectedFieldName(queryBuilder.fieldName());
        double qMinLat = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(queryBuilder.bottomRight().lat()));
        double qMaxLat = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(queryBuilder.topLeft().lat()));
        double qMinLon = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(queryBuilder.topLeft().lon()));
        double qMaxLon = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(queryBuilder.bottomRight().lon()));
        assertEquals(LatLonPoint.newBoxQuery(expectedFieldName, qMinLat, qMaxLat, qMinLon, qMaxLon), indexQuery);
        Query dvQuery = ((IndexOrDocValuesQuery) query).getRandomAccessQuery();
        assertEquals(LatLonDocValuesField.newSlowBoxQuery(expectedFieldName, qMinLat, qMaxLat, qMinLon, qMaxLon), dvQuery);
    }

    public void testValidation() {
        PointTester[] testers = { new TopTester(), new LeftTester(), new BottomTester(), new RightTester() };

        for (PointTester tester : testers) {
            GeoBoundingBoxQueryBuilder builder = createTestQueryBuilder();
            tester.invalidateCoordinate(builder.setValidationMethod(GeoValidationMethod.COERCE), false);
            QueryValidationException except = builder.checkLatLon();
            assertNull(
                "validation w/ coerce should ignore invalid "
                    + tester.getClass().getName()
                    + " coordinate: "
                    + tester.invalidCoordinate
                    + " ",
                except
            );

            tester.invalidateCoordinate(builder.setValidationMethod(GeoValidationMethod.STRICT), false);
            except = builder.checkLatLon();
            assertNotNull(
                "validation w/o coerce should detect invalid coordinate: "
                    + tester.getClass().getName()
                    + " coordinate: "
                    + tester.invalidCoordinate,
                except
            );
        }
    }
}
