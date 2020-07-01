/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

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
package org.elasticsearch.xpack.spatial.common;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A class representing a Bounding-Box for use by bounding box aggregations.
 */
public class CartesianBoundingBox implements ToXContentObject, Writeable {
    public static final ParseField BOUNDS_FIELD = new ParseField("bounds");
    public static final ParseField X_FIELD = new ParseField("x");
    public static final ParseField Y_FIELD = new ParseField("y");
    public static final ParseField TOP_LEFT_FIELD = new ParseField("top_left");
    public static final ParseField BOTTOM_RIGHT_FIELD = new ParseField("bottom_right");

    private final CartesianPoint topLeft;
    private final CartesianPoint bottomRight;

    public CartesianBoundingBox(CartesianPoint topLeft, CartesianPoint bottomRight) {
        this.topLeft = topLeft;
        this.bottomRight = bottomRight;
    }

    public CartesianBoundingBox(StreamInput input) throws IOException {
        this.topLeft = new CartesianPoint(input.readFloat(), input.readFloat());
        this.bottomRight = new CartesianPoint(input.readFloat(), input.readFloat());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeFloat(topLeft.getX());
        out.writeFloat(topLeft.getY());
        out.writeFloat(bottomRight.getX());
        out.writeFloat(bottomRight.getY());
    }

    public boolean isUnbounded() {
        return Float.isInfinite(topLeft.getX()) && Float.isInfinite(topLeft.getY())
            && Float.isInfinite(bottomRight.getX()) && Float.isInfinite(bottomRight.getY());
    }

    public CartesianPoint topLeft() {
        return topLeft;
    }

    public CartesianPoint bottomRight() {
        return bottomRight;
    }

    public float top() {
        return topLeft.getY();
    }

    public float bottom() {
        return bottomRight.getY();
    }

    public float left() {
        return topLeft.getX();
    }

    public float right() {
        return bottomRight.getX();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(BOUNDS_FIELD.getPreferredName());
        toXContentFragment(builder, true);
        builder.endObject();
        return builder;
    }

    public XContentBuilder toXContentFragment(XContentBuilder builder, boolean buildXYFields) throws IOException {
        if (buildXYFields) {
            builder.startObject(TOP_LEFT_FIELD.getPreferredName());
            builder.field(X_FIELD.getPreferredName(), topLeft.getX());
            builder.field(Y_FIELD.getPreferredName(), topLeft.getY());
            builder.endObject();
        } else {
            builder.array(TOP_LEFT_FIELD.getPreferredName(), topLeft.getX(), topLeft.getY());
        }
        if (buildXYFields) {
            builder.startObject(BOTTOM_RIGHT_FIELD.getPreferredName());
            builder.field(X_FIELD.getPreferredName(), bottomRight.getX());
            builder.field(Y_FIELD.getPreferredName(), bottomRight.getY());
            builder.endObject();
        } else {
            builder.array(BOTTOM_RIGHT_FIELD.getPreferredName(), bottomRight.getX(), bottomRight.getY());
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CartesianBoundingBox that = (CartesianBoundingBox) o;
        return topLeft.equals(that.topLeft) &&
            bottomRight.equals(that.bottomRight);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topLeft, bottomRight);
    }

    @Override
    public String toString() {
        return "BBOX (" + topLeft.getX() + ", " + bottomRight.getY() + ", " + topLeft.getY() + ", " + bottomRight.getX() + ")";
    }

}
