/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.search.geo;


/**
 * A geographic range defined by the top left and bottom right {@link Point}s.
 */
public class Range implements Comparable<Range> {
    public Point topLeft;
    public Point bottomRight;

    public Range() {
        topLeft = new Point();
        bottomRight = new Point();
    }

    public Range(Point topLeft, Point bottomRight) {
        this.topLeft = new Point(topLeft);
        this.bottomRight = new Point(bottomRight);
    }

    public Range(double topLat, double leftLon, double bottomLat, double rightLon) {
        this.topLeft = new Point(topLat, leftLon);
        this.bottomRight = new Point(bottomLat, rightLon);
    }

    @Override
    public int compareTo(Range r) {
        int rtn;
        rtn = topLeft.compareTo(r.topLeft);
        if (rtn != 0) return rtn;
        rtn = bottomRight.compareTo(r.bottomRight);
        return rtn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Range range = (Range) o;

        if (range.topLeft == null) {
            if (topLeft != null) return false;
        } else if (!range.topLeft.equals(topLeft)) return false;
        if (range.bottomRight == null) {
            if (bottomRight != null) return false;
        } else if (!range.bottomRight.equals(bottomRight)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = 0;
        if (topLeft != null)
            result = topLeft.hashCode();
        result *= 31;
        if (bottomRight != null)
            result += bottomRight.hashCode();
        return result;
    }

    public String toString() {
        return "[(" + topLeft.lat + ", " + topLeft.lon + ") to (" + bottomRight.lat + ", " + bottomRight.lon + ")]";
    }
}
