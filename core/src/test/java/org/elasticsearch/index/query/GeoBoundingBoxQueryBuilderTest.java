/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query;

import com.spatial4j.core.io.GeohashUtils;
import com.spatial4j.core.shape.Rectangle;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.index.query.GeoBoundingBoxQueryBuilder.Type;
import org.elasticsearch.index.search.geo.InMemoryGeoBoundingBoxQuery;
import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.junit.Test;

import java.io.IOException;

public class GeoBoundingBoxQueryBuilderTest extends BaseQueryTestCase<GeoBoundingBoxQueryBuilder> {
    /** Randomly generate either NaN or one of the two infinity values. */
    private static Double[] invalidDoubles = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};

    @Override
    protected GeoBoundingBoxQueryBuilder doCreateTestQueryBuilder() {
        GeoBoundingBoxQueryBuilder builder = new GeoBoundingBoxQueryBuilder("mapped_geo");
        Rectangle box = RandomShapeGenerator.xRandomRectangle(getRandom(), RandomShapeGenerator.xRandomPoint(getRandom()));

        if (frequently()) {
            // check the top-left/bottom-right combination of setters
            int path = randomIntBetween(0, 2);
            switch (path) {
            case 0:
                builder.topLeft(new GeoPoint(box.getMaxY(), box.getMinX()));
                break;
            case 1:
                builder.topLeft(GeohashUtils.encodeLatLon(box.getMaxY(), box.getMinX()));
                break;
            default:
                builder.topLeft(box.getMaxY(), box.getMinX());
            }

            path = randomIntBetween(0, 2);
            switch (path) {
            case 0:
                builder.bottomRight(new GeoPoint(box.getMinY(), box.getMaxX()));
                break;
            case 1:
                builder.bottomRight(GeohashUtils.encodeLatLon(box.getMinY(), box.getMaxX()));
                break;
            default:
                builder.bottomRight(box.getMinY(), box.getMaxX());
            }
        } else {
            // check the deprecated top-right/bottom-left combination of setters
            int path = randomIntBetween(0, 2);
            switch (path) {
            case 0:
                builder.topRight(new GeoPoint(box.getMaxY(), box.getMaxX()));
                break;
            case 1:
                builder.topRight(GeohashUtils.encodeLatLon(box.getMaxY(), box.getMaxX()));
                break;
            default:
                builder.topRight(box.getMaxY(), box.getMaxX());
            }

            path = randomIntBetween(0, 2);
            switch (path) {
            case 0:
                builder.bottomLeft(new GeoPoint(box.getMinY(), box.getMinX()));
                break;
            case 1:
                builder.bottomLeft(GeohashUtils.encodeLatLon(box.getMinY(), box.getMinX()));
                break;
            default:
                builder.bottomLeft(box.getMinY(), box.getMinX());
            }
        }

        if (randomBoolean()) {
            builder.coerce(randomBoolean());
        }
        if (randomBoolean()) {
            builder.ignoreMalformed(randomBoolean());
        }

        builder.type(randomFrom(Type.values()));
        return builder;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidation_empty_fieldname() {
        new GeoBoundingBoxQueryBuilder("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidation_null_fieldname() {
        new GeoBoundingBoxQueryBuilder(null);
    }

    @Test
    @Override
    public void testToQuery() throws IOException {
        QueryShardContext context = createShardContext();
        if (context.mapperService().smartNameFieldType("mapped_geo", context.getTypes()) == null) {
            try {
                createTestQueryBuilder().toQuery(context);
                fail("Expected exception when there's no geo mapping.");
            } catch(QueryShardException e) {
                // all well
            }
        } else {
            doTestToQuery(context);
        }
    }

    @Test
    public void testValidation() {

        PointTester[] testers = { new TopTester(), new LeftTester(), new BottomTester(), new RightTester() };
        for (PointTester tester : testers) {
            GeoBoundingBoxQueryBuilder builder = createTestQueryBuilder();
            QueryValidationException except = null;
            builder = tester.invalidateCoordinate(builder);
            except = builder.coerce(true).validate(true);
            assertNull("Inner post 2.0 validation w/ coerce should ignore invalid top coordinate: " + tester.getInvalidCoordinate() + " ",
                    except);

            except = builder.coerce(true).validate(false);
            assertNull("Inner pre 2.0 validation w/ coerce should ignore invalid top coordinate: " + tester.getInvalidCoordinate() + " ",
                    except);

            except = builder.coerce(true).validate();
            assertNull("Validation w/ coerce should detect invalid top coordinate: " + tester.getInvalidCoordinate() + " ", except);

            except = builder.coerce(false).ignoreMalformed(false).validate(false);
            assertEquals("Inner post 2.0 validation w/o coerce should ignore invalid top coordinate: " + tester.getInvalidCoordinate(),
                    1,
                    except.validationErrors().size());

            except = builder.coerce(false).ignoreMalformed(false).validate(true);
            assertNull("Inner pre 2.0 validation w/o coerce should ignore invalid top coordinate: " + tester.getInvalidCoordinate(),
                    except);

            except = builder.coerce(false).ignoreMalformed(false).validate();
            assertNull("Validation w/o coerce should ignore invalid top coordinate: " + tester.getInvalidCoordinate(),
                    except);

            double brokenDouble = randomFrom(invalidDoubles);
            tester.breakCoordinate(brokenDouble, builder);
            except = builder.coerce(true).validate(false);
            assertNull("Inner post 2.0 validation should not detect broken coordinates: " + brokenDouble,
                    except);

            except = builder.coerce(true).validate(true);
            assertNull("Inner pre 2.0 validation should not detect broken coordinates: " + brokenDouble,
                    except);

            except = builder.coerce(true).validate();
            assertNull("Validation w/ coerce should detect broken top coordinate: " + brokenDouble, 
                    except);

            except = builder.coerce(false).ignoreMalformed(false).validate(false);
            assertEquals("Inner post 2.0 validation should detect broken coordinates: " + brokenDouble,
                    1,
                    except.validationErrors().size());

            except = builder.coerce(false).ignoreMalformed(false).validate(true);
            assertNull("Inner pre 2.0 validation should not detect broken coordinates: " + brokenDouble,
                    except);

            except = builder.coerce(false).ignoreMalformed(false).validate();
            assertEquals(
                    "Validation w/o normalization should detect broken top coordinate: " + brokenDouble + " " + except.validationErrors(),
                    1, except.validationErrors().size());
        }

        // flipping top and bottom should be detected as error
        GeoBoundingBoxQueryBuilder builder = createTestQueryBuilder();
        double top = builder.topLeft().getLat();
        double left = builder.topLeft().getLon();
        double bottom = builder.bottomRight().getLat();
        double right = builder.bottomRight().getLon();

        builder.topLeft(bottom, left).bottomRight(top, right);

        builder.coerce(true);
        assertNull("Validation w/ coerce should not detect flipped topLeft/bottomRight lat corners: " + "topLeft: " + builder.topLeft() + " vs. bottomRight:"
                + builder.bottomRight() + " ", builder.validate());
        assertNull("Inner validation should not check for flipped lat corners.", builder.validate(true));
        assertNull("Inner validation should not check for flipped lat corners.", builder.validate(false));

        builder.coerce(false).ignoreMalformed(false);
        assertNotNull("Validation should not detect flipped topLeft/bottomRight lat corners: " + "topLeft: " + builder.topLeft() + " vs. bottomRight:"
                + builder.bottomRight() + " ", builder.validate());
        assertNull("Inner validation should not check for flipped lat corners.", builder.validate(true));
        assertNull("Inner validation should not check for flipped lat corners.", builder.validate(false));

        // flipping left and right should be allowed
        builder.topLeft(top, right).bottomRight(bottom, left);
        builder.coerce(true);
        assertNull("Validation should not detect flipped topLeft/bottomRight lon corners: " + "topLeft: " + builder.topLeft() + " vs. bottomRight:"
                + builder.bottomRight() + " ", builder.validate(false));
        assertNull("Inner validation should not check for flipped lat corners.", builder.validate(true));
        assertNull("Inner validation should not check for flipped lat corners.", builder.validate(false));

        builder.coerce(false).ignoreMalformed(false);
        assertNull("Validation should not detect flipped topLeft/bottomRight lon corners: " + "topLeft: " + builder.topLeft() + " vs. bottomRight:"
                + builder.bottomRight() + " ", builder.validate(false));
        assertNull("Inner validation should not check for flipped lat corners.", builder.validate(true));
        assertNull("Inner validation should not check for flipped lat corners.", builder.validate(false));
    }

    @Test
    public void testNormalization() throws IOException {
        GeoBoundingBoxQueryBuilder qb = createTestQueryBuilder();
        if (getCurrentTypes().length != 0 && "mapped_geo".equals(qb.fieldName())) {
            // only execute this test if we are running on a valid geo field
            qb.topLeft(200, 200);
            qb.coerce(true);
            Query query = qb.toQuery(createShardContext());
            if (query instanceof ConstantScoreQuery) {
                ConstantScoreQuery result = (ConstantScoreQuery) query;
                BooleanQuery bboxFilter = (BooleanQuery) result.getQuery();
                for (BooleanClause clause : bboxFilter.clauses()) {
                    NumericRangeQuery boundary = (NumericRangeQuery) clause.getQuery();
                    if (boundary.getMax() != null) {
                        assertTrue("If defined, non of the maximum range values should be larger than 180", boundary.getMax().intValue() <= 180);
                    }
                }
            } else {
                assertTrue("memory queries should result in InMemoryGeoBoundingBoxQuery", query instanceof InMemoryGeoBoundingBoxQuery);
            }
        }
    }

    @Override
    protected void doAssertLuceneQuery(GeoBoundingBoxQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        if (getCurrentTypes().length == 0 || "mapped_geo".equals(queryBuilder.fieldName()) == false) {
            assertTrue("For non geo fields expect MatchNoDocsQuery.", query instanceof MatchNoDocsQuery);
        } else if (queryBuilder.type() == Type.INDEXED) {
            assertTrue("Found no indexed geo query.", query instanceof ConstantScoreQuery);
        } else {
            assertTrue("Found no indexed geo query.", query instanceof InMemoryGeoBoundingBoxQuery);
        }
    }

    // Java really could do with function pointers - is there any Java8 feature that would help me here which I don't know of?
    private abstract class PointTester {
        protected double coordinate;
        
        public double getInvalidCoordinate() {
            return coordinate;
        }
        
        public abstract GeoBoundingBoxQueryBuilder invalidateCoordinate(GeoBoundingBoxQueryBuilder qb);

        public abstract GeoBoundingBoxQueryBuilder breakCoordinate(double coordinate, GeoBoundingBoxQueryBuilder qb);
    }

    private class TopTester extends PointTester {
        private double coordinate = randomDoubleBetween(GeoUtils.MAX_LAT, Double.MAX_VALUE, false);


        @Override
        public GeoBoundingBoxQueryBuilder invalidateCoordinate(GeoBoundingBoxQueryBuilder qb) {
            qb.topLeft(coordinate, qb.topLeft().getLon());
            return qb;
        }

        @Override
        public GeoBoundingBoxQueryBuilder breakCoordinate(double coordinate, GeoBoundingBoxQueryBuilder qb) {
            qb.topLeft(coordinate, qb.topLeft().getLon());
            return qb;
        }
    }

    private class LeftTester extends PointTester {
        private double coordinate = randomDoubleBetween(-Double.MAX_VALUE, GeoUtils.MIN_LON, true);

        @Override
        public GeoBoundingBoxQueryBuilder invalidateCoordinate(GeoBoundingBoxQueryBuilder qb) {
            qb.topLeft(qb.topLeft().getLat(), coordinate);
            return qb;
        }

        @Override
        public GeoBoundingBoxQueryBuilder breakCoordinate(double coordinate, GeoBoundingBoxQueryBuilder qb) {
            qb.topLeft(qb.topLeft().getLat(), coordinate);
            return qb;
        }
    }

    private class BottomTester extends PointTester {
        private double coordinate = randomDoubleBetween(GeoUtils.MAX_LAT, Double.MAX_VALUE, false);

        @Override
        public GeoBoundingBoxQueryBuilder invalidateCoordinate(GeoBoundingBoxQueryBuilder qb) {
            qb.topLeft(coordinate, qb.bottomRight().getLon());
            return qb;
        }

        @Override
        public GeoBoundingBoxQueryBuilder breakCoordinate(double coordinate, GeoBoundingBoxQueryBuilder qb) {
            qb.topLeft(coordinate, qb.bottomRight().getLon());
            return qb;
        }
    }

    private class RightTester extends PointTester {
        private double coordinate = randomDoubleBetween(-Double.MAX_VALUE, GeoUtils.MIN_LON, true);

        @Override
        public GeoBoundingBoxQueryBuilder invalidateCoordinate(GeoBoundingBoxQueryBuilder qb) {
            qb.topLeft(qb.topLeft().getLat(), coordinate);
            return qb;
        }

        @Override
        public GeoBoundingBoxQueryBuilder breakCoordinate(double coordinate, GeoBoundingBoxQueryBuilder qb) {
            qb.topLeft(qb.topLeft().getLat(), coordinate);
            return qb;
        }
    }
}
