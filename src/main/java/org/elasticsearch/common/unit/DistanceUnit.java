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

package org.elasticsearch.common.unit;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 *
 */
public enum DistanceUnit {
    MILES(3959, 24902) {
        @Override
        public String toString() {
            return "miles";
        }

        @Override
        public double toMiles(double distance) {
            return distance;
        }

        @Override
        public double toKilometers(double distance) {
            return distance * MILES_KILOMETRES_RATIO;
        }

        @Override
        public String toString(double distance) {
            return distance + "mi";
        }
    },
    KILOMETERS(6371, 40076) {
        @Override
        public String toString() {
            return "km";
        }

        @Override
        public double toMiles(double distance) {
            return distance / MILES_KILOMETRES_RATIO;
        }

        @Override
        public double toKilometers(double distance) {
            return distance;
        }

        @Override
        public String toString(double distance) {
            return distance + "km";
        }
    };

    static final double MILES_KILOMETRES_RATIO = 1.609344;

    /**
     * Converts the given distance from the given DistanceUnit, to the given DistanceUnit
     *
     * @param distance Distance to convert
     * @param from     Unit to convert the distance from
     * @param to       Unit of distance to convert to
     * @return Given distance converted to the distance in the given uni
     */
    public static double convert(double distance, DistanceUnit from, DistanceUnit to) {
        if (from == to) {
            return distance;
        }
        return (to == MILES) ? distance / MILES_KILOMETRES_RATIO : distance * MILES_KILOMETRES_RATIO;
    }

    public static double parse(String distance, DistanceUnit defaultUnit, DistanceUnit to) {
        if (distance.endsWith("mi")) {
            return convert(Double.parseDouble(distance.substring(0, distance.length() - "mi".length())), MILES, to);
        } else if (distance.endsWith("miles")) {
            return convert(Double.parseDouble(distance.substring(0, distance.length() - "miles".length())), MILES, to);
        } else if (distance.endsWith("km")) {
            return convert(Double.parseDouble(distance.substring(0, distance.length() - "km".length())), KILOMETERS, to);
        } else {
            return convert(Double.parseDouble(distance), defaultUnit, to);
        }
    }

    public static DistanceUnit parseUnit(String distance, DistanceUnit defaultUnit) {
        if (distance.endsWith("mi")) {
            return MILES;
        } else if (distance.endsWith("miles")) {
            return MILES;
        } else if (distance.endsWith("km")) {
            return KILOMETERS;
        } else {
            return defaultUnit;
        }
    }

    protected final double earthCircumference;
    protected final double earthRadius;
    protected final double distancePerDegree;

    DistanceUnit(double earthRadius, double earthCircumference) {
        this.earthCircumference = earthCircumference;
        this.earthRadius = earthRadius;
        this.distancePerDegree = earthCircumference / 360;
    }

    public double getEarthCircumference() {
        return earthCircumference;
    }

    public double getEarthRadius() {
        return earthRadius;
    }

    public double getDistancePerDegree() {
        return distancePerDegree;
    }

    public abstract double toMiles(double distance);

    public abstract double toKilometers(double distance);

    public abstract String toString(double distance);

    public static DistanceUnit fromString(String unit) {
        if ("km".equals(unit)) {
            return KILOMETERS;
        } else if ("mi".equals(unit)) {
            return MILES;
        } else if ("miles".equals(unit)) {
            return MILES;
        }
        throw new ElasticSearchIllegalArgumentException("No distance unit match [" + unit + "]");
    }

    public static void writeDistanceUnit(StreamOutput out, DistanceUnit unit) throws IOException {
        if (unit == MILES) {
            out.writeByte((byte) 0);
        } else if (unit == KILOMETERS) {
            out.writeByte((byte) 1);
        }
    }

    public static DistanceUnit readDistanceUnit(StreamInput in) throws IOException {
        byte b = in.readByte();
        if (b == 0) {
            return MILES;
        } else if (b == 1) {
            return KILOMETERS;
        } else {
            throw new ElasticSearchIllegalArgumentException("No type for distance unit matching [" + b + "]");
        }
    }
}
