/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.geometry.Rectangle;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

public class RectangleMatcher extends BaseMatcher<Rectangle> {
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
    public boolean matches(Object item) {
        if (item instanceof Rectangle other) {
            return Matchers.closeTo(r.getMinX(), error).matches(other.getMinX())
                && Matchers.closeTo(r.getMaxX(), error).matches(other.getMaxX())
                && Matchers.closeTo(r.getMaxY(), error).matches(other.getMaxY())
                && Matchers.closeTo(r.getMinY(), error).matches(other.getMinY());
        }
        return false;
    }

    @Override
    public void describeMismatch(Object item, Description description) {
        description.appendText("was ").appendValue(item);
    }

    @Override
    public void describeTo(Description description) {
        description.appendValue("    RECTANGLE (%s %s %s %s)".formatted(r.getMinX(), r.getMaxX(), r.getMaxY(), r.getMinY()));
    }
}
