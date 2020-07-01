/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.index.fielddata.plain.AbstractLeafPointFieldData;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;

import java.io.IOException;
import java.util.Arrays;

public class ScriptDocValuesPointsTests extends ESTestCase {

    private static MultiPointValues wrap(CartesianPoint[][] points) {
        return new MultiPointValues() {
            CartesianPoint[] current;
            int i;

            @Override
            public CartesianPoint nextValue() {
                return current[i++];
            }

            @Override
            public boolean advanceExact(int docId) {
                if (docId < points.length) {
                    current = points[docId];
                } else {
                    current = new CartesianPoint[0];
                }
                i = 0;
                return current.length > 0;
            }

            @Override
            public int docValueCount() {
                return current.length;
            }
        };
    }

    private static float randomVal() {
        return (float) randomDoubleBetween(-Float.MAX_VALUE, Float.MAX_VALUE, true);
    }

    public void tesGetXY() throws IOException {
        final float x1 = randomVal();
        final float x2 = randomVal();
        final float y1 = randomVal();
        final float y2 = randomVal();

        CartesianPoint[][] points = {{new CartesianPoint(x1, y1), new CartesianPoint(x2, y2)}};
        final MultiPointValues values = wrap(points);
        final AbstractLeafPointFieldData.CartesianPoints script = new AbstractLeafPointFieldData.CartesianPoints(values);

        script.setNextDocId(1);
        assertEquals(true, script.isEmpty());
        script.setNextDocId(0);
        assertEquals(false, script.isEmpty());
        assertEquals(new CartesianPoint(x1, y1), script.getValue());
        assertEquals(x1, script.getX(), 0);
        assertEquals(y1, script.getY(), 0);
        assertTrue(Arrays.equals(new float[] {x1, x2}, script.getXs()));
        assertTrue(Arrays.equals(new float[] {y1, y2}, script.getYs()));
    }

    public void testDistance() throws IOException {
        final float x = randomVal();
        final float y = randomVal();
        CartesianPoint[][] points = {{new CartesianPoint(x, y)}};
        final MultiPointValues values = wrap(points);
        final AbstractLeafPointFieldData.CartesianPoints script = new AbstractLeafPointFieldData.CartesianPoints(values);
        script.setNextDocId(0);

        CartesianPoint[][] points2 = {new CartesianPoint[0]};
        final AbstractLeafPointFieldData.CartesianPoints emptyScript =
            new AbstractLeafPointFieldData.CartesianPoints(wrap(points2));
        emptyScript.setNextDocId(0);

        assertEquals(0d, script.distance(x, y), 0d);
        assertEquals(0d, script.distance(x, y), 0d);

        final float otherX = randomVal();
        final float otherY = randomVal();

        double deltaX = (double) otherX - x;
        double deltaY = (double) otherY - y;
        double distance = Math.sqrt(deltaX * deltaX + deltaY * deltaY);

        assertEquals(distance, script.distance(otherX, otherY), 0d);
    }

    public void testMissingValues() throws IOException {
        CartesianPoint[][] points = new CartesianPoint[between(3, 10)][];
        for (int d = 0; d < points.length; d++) {
            points[d] = new CartesianPoint[randomBoolean() ? 0 : between(1, 10)];
            for (int i = 0; i< points[d].length; i++) {
                points[d][i] =  new CartesianPoint(randomVal(), randomVal());
            }
        }
        final AbstractLeafPointFieldData.CartesianPoints cartesianPoints =
            new AbstractLeafPointFieldData.CartesianPoints(wrap(points));
        for (int d = 0; d < points.length; d++) {
            cartesianPoints.setNextDocId(d);
            if (points[d].length > 0) {
                assertEquals(points[d][0], cartesianPoints.getValue());
            } else {
                Exception e = expectThrows(IllegalStateException.class, () -> cartesianPoints.getValue());
                assertEquals("A document doesn't have a value for a field! " +
                    "Use doc[<field>].size()==0 to check if a document is missing a field!", e.getMessage());
                e = expectThrows(IllegalStateException.class, () -> cartesianPoints.get(0));
                assertEquals("A document doesn't have a value for a field! " +
                    "Use doc[<field>].size()==0 to check if a document is missing a field!", e.getMessage());
            }
            assertEquals(points[d].length, cartesianPoints.size());
            for (int i = 0; i < points[d].length; i++) {
                assertEquals(points[d][i], cartesianPoints.get(i));
            }
        }
    }
}
