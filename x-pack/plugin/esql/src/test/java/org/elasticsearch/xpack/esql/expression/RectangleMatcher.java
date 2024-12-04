/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.compute.aggregation.spatial.PointType;
import org.elasticsearch.geometry.Rectangle;
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;

public class RectangleMatcher extends TypeSafeMatcher<Rectangle> {
    private final Rectangle r;
    private final PointType pointType;
    private final double error;

    public static TypeSafeMatcher<Rectangle> closeTo(Rectangle r, double error, PointType pointType) {
        return new RectangleMatcher(r, error, pointType);
    }

    private RectangleMatcher(Rectangle r, double error, PointType pointType) {
        this.r = r;
        this.pointType = pointType;
        this.error = error;
    }

    @Override
    protected boolean matchesSafely(Rectangle other) {
        // For geo bounds, longitude of (-180, 180) and (-epsilon, epsilon) are actually very close, since both encompass the entire globe.
        boolean wrapAroundWorkAround = pointType == PointType.GEO && r.getMinX() >= r.getMaxX();
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
