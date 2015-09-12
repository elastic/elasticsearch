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

package org.elasticsearch.common.geo;

import java.util.Locale;

/**
 * Enum representing the relationship between a Query / Filter Shape and indexed Shapes
 * that will be used to determine if a Document should be matched or not
 */
public enum ShapeRelation {

    INTERSECTS("intersects"),
    DISJOINT("disjoint"),
    WITHIN("within");

    private final String relationName;

    ShapeRelation(String relationName) {
        this.relationName = relationName;
    }

    public static ShapeRelation getRelationByName(String name) {
        name = name.toLowerCase(Locale.ENGLISH);
        for (ShapeRelation relation : ShapeRelation.values()) {
            if (relation.relationName.equals(name)) {
                return relation;
            }
        }
        return null;
    }

    public String getRelationName() {
        return relationName;
    }
}
