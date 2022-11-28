/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.test.ESTestCase;

public class BigLongDoubleDoubleArrayTests extends ESTestCase {

    public void testBasic() {
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        var array = new BigLongDoubleDoubleArray(3, bigArrays, false);

        array.set(0, 1, 2, 3);
        array.set(1, 9, 8, 7);
        array.set(2, 4, 5, 6);

        assertEquals(1L, array.getLong0(0));
        assertEquals(2d, array.getDouble0(0), 0.0d);
        assertEquals(3d, array.getDouble1(0), 0.0d);

        assertEquals(9L, array.getLong0(1));
        assertEquals(8d, array.getDouble0(1), 0.0d);
        assertEquals(7d, array.getDouble1(1), 0.0d);

        assertEquals(4L, array.getLong0(2));
        assertEquals(5d, array.getDouble0(2), 0.0d);
        assertEquals(6d, array.getDouble1(2), 0.0d);

    }
}
