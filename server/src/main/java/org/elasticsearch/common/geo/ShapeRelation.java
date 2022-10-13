/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import org.apache.lucene.document.ShapeField.QueryRelation;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

/**
 * Enum representing the relationship between a Query / Filter Shape and indexed Shapes
 * that will be used to determine if a Document should be matched or not
 */
public enum ShapeRelation implements Writeable {

    INTERSECTS("intersects"),
    DISJOINT("disjoint"),
    WITHIN("within"),
    CONTAINS("contains");

    private final String relationName;

    ShapeRelation(String relationName) {
        this.relationName = relationName;
    }

    public static ShapeRelation readFromStream(StreamInput in) throws IOException {
        return in.readEnum(ShapeRelation.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
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

    /** Maps ShapeRelation to Lucene's LatLonShapeRelation */
    public QueryRelation getLuceneRelation() {
        return switch (this) {
            case INTERSECTS -> QueryRelation.INTERSECTS;
            case DISJOINT -> QueryRelation.DISJOINT;
            case WITHIN -> QueryRelation.WITHIN;
            case CONTAINS -> QueryRelation.CONTAINS;
        };
    }

    public String getRelationName() {
        return relationName;
    }
}
