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

package org.elasticsearch.geo.geometry;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Created by nknize on 9/22/17.
 */
public enum ShapeType {
    POINT("point", 1),
    MULTIPOINT("multipoint", 4),
    LINESTRING("linestring", 2),
    MULTILINESTRING("multilinestring", 5),
    POLYGON("polygon", 3),
    MULTIPOLYGON("multipolygon", 6),
    GEOMETRYCOLLECTION("geometrycollection", 7),
    ENVELOPE("envelope", 8), // not part of the actual WKB spec
    CIRCLE("circle", 9); // not part of the actual WKB spec

    private final String shapeName;
    private final int wkbOrdinal;
    private static Map<String, ShapeType> shapeTypeMap = new HashMap<>();
    private static Map<Integer, ShapeType> wkbTypeMap = new HashMap<>();
    private static final String BBOX = "BBOX";

    static {
        for (ShapeType type : values()) {
            shapeTypeMap.put(type.shapeName, type);
            wkbTypeMap.put(type.wkbOrdinal, type);
        }
        shapeTypeMap.put(ENVELOPE.wktName().toLowerCase(Locale.ROOT), ENVELOPE);
    }

    ShapeType(String shapeName, int wkbOrdinal) {
        this.shapeName = shapeName;
        this.wkbOrdinal = wkbOrdinal;
    }

    protected String typename() {
        return shapeName;
    }

    /**
     * wkt shape name
     */
    public String wktName() {
        return this == ENVELOPE ? BBOX : this.shapeName;
    }

    public int wkbOrdinal() {
        return this.wkbOrdinal;
    }

    public static ShapeType forName(String shapename) {
        String typename = shapename.toLowerCase(Locale.ROOT);
        for (ShapeType type : values()) {
            if (type.shapeName.equals(typename)) {
                return type;
            }
        }
        throw new IllegalArgumentException("unknown geo_shape [" + shapename + "]");
    }
}
