/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.geo;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A class representing a Geo-Bounding-Box for use by Geo queries and aggregations
 * that deal with extents/rectangles representing rectangular areas of interest.
 */
public abstract class BoundingBox<T extends ToXContentFragment> implements ToXContentFragment, Writeable {
    protected final T topLeft;
    protected final T bottomRight;

    public BoundingBox(T topLeft, T bottomRight) {
        this.topLeft = topLeft;
        this.bottomRight = bottomRight;
    }

    public boolean isUnbounded() {
        return Double.isNaN(left()) || Double.isNaN(top()) || Double.isNaN(right()) || Double.isNaN(bottom());
    }

    public T topLeft() {
        return topLeft;
    }

    public T bottomRight() {
        return bottomRight;
    }

    public abstract double top();

    public abstract double bottom();

    public abstract double left();

    public abstract double right();

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragment(builder, true);
        builder.endObject();
        return builder;
    }

    public abstract XContentBuilder toXContentFragment(XContentBuilder builder, boolean buildLatLonFields) throws IOException;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BoundingBox<?> that = (BoundingBox<?>) o;
        return topLeft.equals(that.topLeft) && bottomRight.equals(that.bottomRight);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topLeft, bottomRight);
    }

    @Override
    public String toString() {
        return "BBOX (" + left() + ", " + right() + ", " + top() + ", " + bottom() + ")";
    }
}
