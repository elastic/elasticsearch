/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.lucene.document;

import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.XTessellator;
import org.apache.lucene.util.BytesRef;

import java.util.ArrayList;
import java.util.List;

import static org.apache.lucene.document.ShapeField.BYTES;
import static org.apache.lucene.document.ShapeField.TYPE;
import static org.apache.lucene.document.ShapeField.encodeTriangle;

/**
 * This is a copy of the same class from Lucene used here as a way to access a modified version of Tessellator class.
 * Once lucene releases with the bugfix to Tessellator, we can remove both these classes.
 */
public class XLatLonShape {

    // no instance:
    private XLatLonShape() {}

    /** create indexable fields for polygon geometry */
    public static Field[] createIndexableFields(String fieldName, Polygon polygon) {
        // the lionshare of the indexing is done by the tessellator
        List<XTessellator.Triangle> tessellation = XTessellator.tessellate(polygon);
        List<Triangle> fields = new ArrayList<>();
        for (XTessellator.Triangle t : tessellation) {
            fields.add(new Triangle(fieldName, t));
        }
        return fields.toArray(new Field[fields.size()]);
    }

    public static class Triangle extends Field {

        /** xtor from a given Tessellated Triangle object */
        Triangle(String name, XTessellator.Triangle t) {
            super(name, TYPE);
            setTriangleValue(
                t.getEncodedX(0),
                t.getEncodedY(0),
                t.isEdgefromPolygon(0),
                t.getEncodedX(1),
                t.getEncodedY(1),
                t.isEdgefromPolygon(1),
                t.getEncodedX(2),
                t.getEncodedY(2),
                t.isEdgefromPolygon(2)
            );
        }

        /** sets the vertices of the triangle as integer encoded values */
        protected void setTriangleValue(
            int aX,
            int aY,
            boolean abFromShape,
            int bX,
            int bY,
            boolean bcFromShape,
            int cX,
            int cY,
            boolean caFromShape
        ) {
            final byte[] bytes;

            if (fieldsData == null) {
                bytes = new byte[7 * BYTES];
                fieldsData = new BytesRef(bytes);
            } else {
                bytes = ((BytesRef) fieldsData).bytes;
            }
            encodeTriangle(bytes, aY, aX, abFromShape, bY, bX, bcFromShape, cY, cX, caFromShape);
        }
    }
}
