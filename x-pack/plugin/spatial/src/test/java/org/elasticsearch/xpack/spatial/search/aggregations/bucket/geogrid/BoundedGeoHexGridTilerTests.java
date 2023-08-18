/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.common.H3CartesianUtil;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

import static org.elasticsearch.common.geo.GeoUtils.normalizeLon;
import static org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.GeoHexGridTiler.BoundedGeoHexGridTiler.height;
import static org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.GeoHexGridTiler.BoundedGeoHexGridTiler.width;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

public class BoundedGeoHexGridTilerTests extends ESTestCase {

    public void testBoundedTilerInflation() {
        for (int i = 0; i < 10000; i++) {
            Point point = GeometryTestUtils.randomPoint();
            int precision = random().nextInt(0, 1);
            assertH3CellInflation(point, precision);
        }
    }

    /** Test with cell bounds crossing dateline */
    public void testBoundedTilerInflation_805bfffffffffff() {
        for (int precision = 0; precision < 2; precision++) {
            long h3 = H3.stringToH3("805bfffffffffff");
            LatLng centroid = H3.h3ToLatLng(h3);
            assertH3CellInflation(new Point(centroid.getLonDeg(), centroid.getLatDeg()), precision);
        }
    }

    /** Test for north pole with bounds width 360 */
    public void testBoundedTilerInflation_8001fffffffffff() {
        for (int precision = 0; precision < 2; precision++) {
            long h3 = H3.stringToH3("8001fffffffffff");
            LatLng centroid = H3.h3ToLatLng(h3);
            assertH3CellInflation(new Point(centroid.getLonDeg(), centroid.getLatDeg()), precision);
        }
    }

    /** Test with inner bbox crossing dateline */
    public void testBoundedTilerInflation_80bbfffffffffff() {
        for (int precision = 0; precision < 2; precision++) {
            long h3 = H3.stringToH3("80bbfffffffffff");
            LatLng centroid = H3.h3ToLatLng(h3);
            assertH3CellInflation(new Point(centroid.getLonDeg(), centroid.getLatDeg()), precision);
        }
    }

    /** Test with inner bbox on different side of dateline than inflated bbox */
    public void testBoundedTilerInflation_80ebfffffffffff() {
        for (int precision = 0; precision < 2; precision++) {
            long h3 = H3.stringToH3("80ebfffffffffff");
            LatLng centroid = H3.h3ToLatLng(h3);
            assertH3CellInflation(new Point(centroid.getLonDeg(), centroid.getLatDeg()), precision);
        }
    }

    /** Test with inner bbox and cell crossing line of zero longitude */
    public void testBoundedTilerInflation_8075fffffffffff() {
        for (int precision = 0; precision < 2; precision++) {
            long h3 = H3.stringToH3("8075fffffffffff");
            LatLng centroid = H3.h3ToLatLng(h3);
            assertH3CellInflation(new Point(centroid.getLonDeg(), centroid.getLatDeg()), precision);
        }
    }

    private void assertH3CellInflation(Point point, int precision) {
        long h3 = H3.geoToH3(point.getLat(), point.getLon(), precision);
        LatLng centroid = H3.h3ToLatLng(h3);
        String cell = "H3[" + H3.h3ToString(h3) + "]:" + precision;
        Rectangle cellBounds = H3CartesianUtil.toBoundingBox(h3);
        boolean cellCrossesDateline = cellBounds.getMinX() > cellBounds.getMaxX();
        double maxX = cellCrossesDateline ? 360 + cellBounds.getMaxX() : cellBounds.getMaxX();
        double centroidLeft = centroid.getLonDeg();
        double centroidRight = centroid.getLonDeg();
        if (cellCrossesDateline) {
            centroidLeft = normLonAgainst(centroid.getLonDeg(), cellBounds.getMinX());
            centroidRight = normLonAgainst(centroid.getLonDeg(), maxX);
        }
        double left = normalizeLon(wAvg(centroidLeft, cellBounds.getMinX(), 0.1));
        double right = normalizeLon(wAvg(centroidRight, maxX, 0.1));
        double top = wAvg(centroid.getLatDeg(), cellBounds.getMaxY(), 0.1);
        double bottom = wAvg(centroid.getLatDeg(), cellBounds.getMinY(), 0.1);
        GeoBoundingBox bbox = new GeoBoundingBox(new GeoPoint(top, left), new GeoPoint(bottom, right));

        // Inflating with factor 0.0 should match the original bounding box
        GeoBoundingBox inflated0 = inflateBbox(precision, bbox, 0.0);
        assertThat(cell + " Full-inflated", inflated0, matchesBounds(bbox));

        // Test inflation factors 1.0 should match the outer cell bounds
        GeoBoundingBox inflated1 = inflateBbox(precision, bbox, 1.0);
        Rectangle fullyInflated = getFullBounds(cellBounds, bbox);
        assertThat(cell + " Full-inflated", inflated1, matchesBounds(fullyInflated));

        // Now test a range of inflation factors
        for (double factor = 0.1; factor <= 0.95; factor += 0.1) {
            GeoBoundingBox inflated = inflateBbox(precision, bbox, factor);
            // Compare inflated to outer bounds (should be smaller)
            assertThat(cell + " Inflated(" + factor + ")", inflated, withinBounds(fullyInflated));
            // Compare inflated to inner bounds (should be larger)
            assertThat(cell + " Inflated(" + factor + ")", inflated, containsBounds(bbox));
        }
    }

    /** Calculate weighted average: 1 means second dominates, 0 means first dominates */
    private static double wAvg(double first, double second, double weight) {
        double width = (second - first) * weight;
        return first + width;
    }

    // We usually got longitude values the same side of the dateline by adding 360, never subtracting 360
    private static double normLonAgainst(double value, double other) {
        if (other > 0 && value < 0) {
            value += 360;
        }
        return value;
    }

    private GeoBoundingBox inflateBbox(int precision, GeoBoundingBox bbox, double factor) {
        return GeoHexGridTiler.BoundedGeoHexGridTiler.inflateBbox(precision, bbox, factor);
    }

    /* Calculate the bounds of the h3 cell assuming the test bbox is entirely within the cell */
    private Rectangle getFullBounds(Rectangle bounds, GeoBoundingBox bbox) {
        final double height = height(bounds);
        final double width = width(bounds);
        // inflate the coordinates by the full width and height
        final double minY = Math.max(bbox.bottom() - height, -90d);
        final double maxY = Math.min(bbox.top() + height, 90d);
        final double left = GeoUtils.normalizeLon(bbox.left() - width);
        final double right = GeoUtils.normalizeLon(bbox.right() + width);
        if (2 * width + width(bbox) >= 360d) {
            // if the total width is bigger than the world, then it covers all longitude range.
            return new Rectangle(-180, 180, maxY, minY);
        } else {
            return new Rectangle(left, right, maxY, minY);
        }
    }

    public void testBoundsMatcher() {
        GeoBoundingBox bounds = new GeoBoundingBox(new GeoPoint(10, -10), new GeoPoint(-10, 10));
        GeoBoundingBox bigger = new GeoBoundingBox(new GeoPoint(20, -20), new GeoPoint(-20, 20));
        GeoBoundingBox smaller = new GeoBoundingBox(new GeoPoint(5, -5), new GeoPoint(-5, 5));
        GeoBoundingBox intersecting = new GeoBoundingBox(new GeoPoint(20, -20), new GeoPoint(0, 20));
        GeoBoundingBox nonIntersecting = new GeoBoundingBox(new GeoPoint(20, -10), new GeoPoint(15, 10));
        {
            assertThat("equals", bounds, matchesBounds(bounds));
            assertThat("contains", bounds, containsBounds(smaller));
            assertThat("within", bounds, withinBounds(bigger));
        }
        {
            assertThat("not equals", bounds, not(matchesBounds(smaller)));
            assertThat("not equals", bounds, not(matchesBounds(bigger)));
            assertThat("not equals", bounds, not(matchesBounds(intersecting)));
            assertThat("not equals", bounds, not(matchesBounds(nonIntersecting)));
        }
        {
            assertThat("does not contain", bounds, not(containsBounds(bigger)));
            assertThat("does not contain", bounds, not(containsBounds(intersecting)));
            assertThat("does not contain", bounds, not(containsBounds(nonIntersecting)));
        }
        {
            assertThat("is not within", bounds, not(withinBounds(smaller)));
            assertThat("is not within", bounds, not(withinBounds(intersecting)));
            assertThat("is not within", bounds, not(withinBounds(nonIntersecting)));
        }
    }

    private static TestCompareBounds matchesBounds(GeoBoundingBox other) {
        return new TestCompareBounds(other, 0);
    }

    private static TestCompareBounds matchesBounds(Rectangle other) {
        return new TestCompareBounds(other, 0);
    }

    private static TestCompareBounds containsBounds(GeoBoundingBox other) {
        return new TestCompareBounds(other, 1);
    }

    private static TestCompareBounds withinBounds(Rectangle other) {
        return new TestCompareBounds(other, -1);
    }

    private static TestCompareBounds withinBounds(GeoBoundingBox other) {
        return new TestCompareBounds(other, -1);
    }

    private static class TestCompareBounds extends BaseMatcher<GeoBoundingBox> {

        private final GeoBoundingBox other;
        private final int comparison;
        private boolean matchedTop;
        private boolean matchedBottom;
        private boolean matchedLeft;
        private boolean matchedRight;

        private TestCompareBounds(Rectangle rect, int comparison) {
            this.other = new GeoBoundingBox(new GeoPoint(rect.getMaxY(), rect.getMinX()), new GeoPoint(rect.getMinY(), rect.getMaxX()));
            this.comparison = comparison;
        }

        private TestCompareBounds(GeoBoundingBox other, int comparison) {
            this.other = other;
            this.comparison = comparison;
        }

        @Override
        public boolean matches(Object actual) {
            if (actual instanceof GeoBoundingBox bbox) {
                if (comparison == 0) {
                    matchedTop = closeTo(bbox.top(), 1e-10).matches(other.top());
                    matchedBottom = closeTo(bbox.bottom(), 1e-10).matches(other.bottom());
                    matchedLeft = closeTo(posLon(bbox.left()), 1e-10).matches(posLon(other.left()));
                    matchedRight = closeTo(posLon(bbox.right()), 1e-10).matches(posLon(other.right()));
                } else {
                    if (comparison > 0) {
                        // assert that 'bbox' is larger than and entirely contains 'other'
                        setBoxWithinBox(other, bbox);
                    } else {
                        // assert that 'bbox' is smaller than and entirely contained within 'other'
                        setBoxWithinBox(bbox, other);
                    }
                }
                return matchedTop && matchedBottom && matchedLeft && matchedRight;
            }
            return false;
        }

        private void setBoxWithinBox(GeoBoundingBox smaller, GeoBoundingBox larger) {
            double smallerRight = smaller.right();
            double largerRight = larger.right();
            double smallerLeft = smaller.left();
            double largerLeft = larger.left();
            boolean smallerCrossesDateline = smaller.left() > smaller.right();
            boolean largerCrossesDateline = larger.left() > larger.right();
            if (smallerCrossesDateline || largerCrossesDateline) {
                smallerRight = smallerCrossesDateline ? smallerRight + 360 : smallerRight;
                largerRight = largerCrossesDateline ? largerRight + 360 : largerRight;
                if (smallerCrossesDateline == false && smallerLeft < 0) {
                    // Larger crosses dateline, but smaller does not, make sure they have comparable signs
                    smallerLeft += 360;
                    smallerRight += 360;
                }
                if (largerCrossesDateline == false && largerLeft < 0) {
                    // Smaller crosses dateline, but larger does not, make sure they have comparable signs
                    largerLeft += 360;
                    largerRight += 360;
                }
            }
            assert smallerLeft < smallerRight;
            assert largerLeft < largerRight;
            // assert that 'smaller' is smaller than and entirely contained within 'larger'
            matchedTop = larger.top() >= 90 || greaterThan(smaller.top()).matches(larger.top());
            matchedBottom = larger.bottom() <= -90 || lessThan(smaller.bottom()).matches(larger.bottom());
            // Only check smaller longitudes are within larger longitudes if larger does not cover all longitudes (eg. polar cells)
            boolean largerGlobalLongitude = largerRight - largerLeft >= 360;
            matchedLeft = largerGlobalLongitude || lessThan(smallerLeft).matches(largerLeft);
            matchedRight = largerGlobalLongitude || greaterThan(smallerRight).matches(largerRight);
        }

        private static double posLon(double value) {
            if (value < 0) return value + 360;
            return value;
        }

        @Override
        public void describeTo(Description description) {
            if (comparison == 0) {
                description.appendText("To match " + other);
            } else if (comparison > 0) {
                description.appendText("To contain " + other);
            } else {
                description.appendText("To be contained by " + other);
            }
        }

        @Override
        public void describeMismatch(Object item, Description description) {
            super.describeMismatch(item, description);
            if (item instanceof GeoBoundingBox bbox) {
                if (matchedTop == false) {
                    describeMismatchOf(description, "top", other.top(), bbox.top(), true);
                }
                if (matchedBottom == false) {
                    describeMismatchOf(description, "bottom", other.bottom(), bbox.bottom(), false);
                }
                if (matchedLeft == false) {
                    describeMismatchOf(description, "left", other.left(), bbox.left(), false);
                }
                if (matchedRight == false) {
                    describeMismatchOf(description, "right", other.right(), bbox.right(), true);
                }
            }
        }

        private void describeMismatchOf(Description description, String field, double thisValue, double thatValue, boolean max) {
            description.appendText("\n\t and " + field + " ");
            description.appendValue(thisValue).appendText(" was not").appendText(describeComparison(max)).appendValue(thatValue);
        }

        private String describeComparison(boolean max) {
            return switch (comparison) {
                case 0 -> " equal to ";
                case -1 -> max ? " greater than " : " less than ";
                case 1 -> max ? " less than " : " greater than ";
                default -> " UNKNOWN ";
            };
        }
    }
}
