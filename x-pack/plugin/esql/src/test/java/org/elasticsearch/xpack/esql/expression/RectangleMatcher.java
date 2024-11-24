/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.geometry.Rectangle;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;

public class RectangleMatcher extends TypeSafeMatcher<Rectangle> {
    private final Rectangle r;
    private final double error;

    public static Matcher<Rectangle> closeTo(Rectangle r, double error) {
        return new RectangleMatcher(r, error);
    }

    private RectangleMatcher(Rectangle r, double error) {
        this.r = r;
        this.error = error;
    }

    @Override
    protected boolean matchesSafely(Rectangle other) {
        return Matchers.closeTo(r.getMinX(), error).matches(other.getMinX())
            && Matchers.closeTo(r.getMaxX(), error).matches(other.getMaxX())
            && Matchers.closeTo(r.getMaxY(), error).matches(other.getMaxY())
            && Matchers.closeTo(r.getMinY(), error).matches(other.getMinY());
    }

    @Override
    public void describeMismatchSafely(Rectangle rectangle, Description description) {
        description.appendText("was ").appendValue(rectangle);
    }

    @Override
    public void describeTo(Description description) {
        description.appendValue("    BBOX" + r);
    }
}
