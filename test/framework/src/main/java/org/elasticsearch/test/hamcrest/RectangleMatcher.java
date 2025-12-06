/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.hamcrest;

import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;

/**
 * Example usage: <code>assertThat(actualRectangle, RectangleMatcher.closeTo(expectedRectangle, 0.0001, PointType.CARTESIAN));</code>, or it
 * can be used as a parameter to {@link WellKnownBinaryBytesRefMatcher}.
 */
public class RectangleMatcher extends TypeSafeMatcher<Rectangle> {
    private final Rectangle r;
    private final CoordinateEncoder coordinateEncoder;
    private final double error;

    public static TypeSafeMatcher<Rectangle> closeTo(Rectangle r, double error, CoordinateEncoder coordinateEncoder) {
        return new RectangleMatcher(r, error, coordinateEncoder);
    }

    private RectangleMatcher(Rectangle r, double error, CoordinateEncoder coordinateEncoder) {
        this.r = r;
        this.coordinateEncoder = coordinateEncoder;
        this.error = error;
    }

    /**
     * Casts the rectangle coordinates to floats before comparing. Useful when working with extents which hold the coordinate data as ints.
     */
    public static TypeSafeMatcher<Rectangle> closeToFloat(Rectangle r, double v, CoordinateEncoder encoder) {
        var normalized = new Rectangle((float) r.getMinX(), (float) r.getMaxX(), (float) r.getMaxY(), (float) r.getMinY());
        return closeTo(normalized, v, encoder);
    }

    @Override
    protected boolean matchesSafely(Rectangle other) {
        // For geo bounds, longitude of (-180, 180) and (epsilon, -epsilon) are actually very close, since both encompass the entire globe.
        boolean wrapAroundWorkAround = coordinateEncoder == CoordinateEncoder.GEO && r.getMinX() >= r.getMaxX();
        boolean matchMinX = Matchers.closeTo(r.getMinX(), error).matches(other.getMinX())
            || (wrapAroundWorkAround && Matchers.closeTo(r.getMinX() - 180, error).matches(other.getMinX()))
            || (wrapAroundWorkAround && Matchers.closeTo(r.getMinX(), error).matches(other.getMinX() - 180));
        boolean matchMaxX = Matchers.closeTo(r.getMaxX(), error).matches(other.getMaxX())
            || (wrapAroundWorkAround && Matchers.closeTo(r.getMaxX() + 180, error).matches(other.getMaxX()))
            || (wrapAroundWorkAround && Matchers.closeTo(r.getMaxX(), error).matches(other.getMaxX() + 180));

        return matchMinX
            && matchMaxX
            && Matchers.closeTo(r.getMaxY(), error).matches(other.getMaxY())
            && Matchers.closeTo(r.getMinY(), error).matches(other.getMinY());
    }

    @Override
    public void describeMismatchSafely(Rectangle rectangle, Description description) {
        description.appendText("was ").appendValue(rectangle);
    }

    @Override
    public void describeTo(Description description) {
        description.appendValue("    " + r);
    }
}
