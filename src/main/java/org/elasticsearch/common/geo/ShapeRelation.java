package org.elasticsearch.common.geo;

import java.util.Locale;

/**
 * Enum representing the relationship between a Query / Filter Shape and indexed Shapes
 * that will be used to determine if a Document should be matched or not
 */
public enum ShapeRelation {

    INTERSECTS("intersects"),
    DISJOINT("disjoint"),
    CONTAINS("contains");

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
