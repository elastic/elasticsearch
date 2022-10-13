/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.util.Collections;
import java.util.Map;

public class BufferTests extends ScriptTestCase {

    public void testByteBufferMethods() {
        ByteBuffer bb = ByteBuffer.wrap(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7 });
        Map<String, Object> params = Collections.singletonMap("bb", ByteBuffer.wrap(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7 }));

        assertEquals(bb.limit(), exec("def bb = params['bb']; bb.limit()", params, true));
        assertEquals(bb.limit(), exec("ByteBuffer bb = params['bb']; bb.limit()", params, true));

        assertEquals(bb.order(), exec("def bb = params['bb']; bb.order()", params, true));
        assertEquals(bb.order(), exec("ByteBuffer bb = params['bb']; bb.order()", params, true));

        assertEquals(
            bb.order(ByteOrder.LITTLE_ENDIAN).order(),
            exec("def bb = params['bb']; bb.order(ByteOrder.LITTLE_ENDIAN).order()", params, true)
        );
        assertEquals(
            bb.order(ByteOrder.LITTLE_ENDIAN).order(),
            exec("ByteBuffer bb = params['bb']; bb.order(ByteOrder.LITTLE_ENDIAN).order()", params, true)
        );

        assertEquals(bb.get(0), exec("def bb = params['bb']; bb.get(0)", params, true));
        assertEquals(bb.get(0), exec("ByteBuffer bb = params['bb']; bb.get(0)", params, true));
        assertEquals(bb.get(1), exec("def bb = params['bb']; bb.get(1)", params, true));
        assertEquals(bb.get(1), exec("ByteBuffer bb = params['bb']; bb.get(1)", params, true));

        assertEquals(bb.getChar(0), exec("def bb = params['bb']; bb.getChar(0)", params, true));
        assertEquals(bb.getChar(0), exec("ByteBuffer bb = params['bb']; bb.getChar(0)", params, true));

        assertEquals(bb.getDouble(0), (double) exec("def bb = params['bb']; bb.getDouble(0)", params, true), 0.1);
        assertEquals(bb.getDouble(0), (double) exec("ByteBuffer bb = params['bb']; bb.getDouble(0)", params, true), 0.1);

        assertEquals(bb.getFloat(0), (float) exec("def bb = params['bb']; bb.getFloat(0)", params, true), 0.1);
        assertEquals(bb.getFloat(0), (float) exec("ByteBuffer bb = params['bb']; bb.getFloat(0)", params, true), 0.1);

        assertEquals(bb.getInt(0), exec("def bb = params['bb']; bb.getInt(0)", params, true));
        assertEquals(bb.getInt(0), exec("ByteBuffer bb = params['bb']; bb.getInt(0)", params, true));

        assertEquals(bb.getLong(0), exec("def bb = params['bb']; bb.getLong(0)", params, true));
        assertEquals(bb.getLong(0), exec("ByteBuffer bb = params['bb']; bb.getLong(0)", params, true));

        assertEquals(bb.getShort(0), exec("def bb = params['bb']; bb.getShort(0)", params, true));
        assertEquals(bb.getShort(0), exec("ByteBuffer bb = params['bb']; bb.getShort(0)", params, true));

        assertEquals(bb.asCharBuffer(), exec("def bb = params['bb']; bb.asCharBuffer()", params, true));
        assertEquals(bb.asCharBuffer(), exec("ByteBuffer bb = params['bb']; bb.asCharBuffer()", params, true));

        assertEquals(bb.asDoubleBuffer(), exec("def bb = params['bb']; bb.asDoubleBuffer()", params, true));
        assertEquals(bb.asDoubleBuffer(), exec("ByteBuffer bb = params['bb']; bb.asDoubleBuffer()", params, true));

        assertEquals(bb.asFloatBuffer(), exec("def bb = params['bb']; bb.asFloatBuffer()", params, true));
        assertEquals(bb.asFloatBuffer(), exec("ByteBuffer bb = params['bb']; bb.asFloatBuffer()", params, true));

        assertEquals(bb.asIntBuffer(), exec("def bb = params['bb']; bb.asIntBuffer()", params, true));
        assertEquals(bb.asIntBuffer(), exec("ByteBuffer bb = params['bb']; bb.asIntBuffer()", params, true));

        assertEquals(bb.asLongBuffer(), exec("def bb = params['bb']; bb.asLongBuffer()", params, true));
        assertEquals(bb.asLongBuffer(), exec("ByteBuffer bb = params['bb']; bb.asLongBuffer()", params, true));

        assertEquals(bb.asShortBuffer(), exec("def bb = params['bb']; bb.asShortBuffer()", params, true));
        assertEquals(bb.asShortBuffer(), exec("ByteBuffer bb = params['bb']; bb.asShortBuffer()", params, true));

        assertEquals(ByteBuffer.wrap(new byte[] { 1, 2, 3 }), exec("ByteBuffer.wrap(new byte[] {1, 2, 3})"));
        assertEquals(ByteBuffer.wrap(new byte[] { 1, 2, 3 }, 1, 2), exec("ByteBuffer.wrap(new byte[] {1, 2, 3}, 1, 2)"));
    }

    public void testCharBufferMethods() {
        CharBuffer bb = CharBuffer.wrap(new char[] { 0, 1, 2, 3, 4, 5, 6, 7 });
        Map<String, Object> params = Collections.singletonMap("bb", bb);

        assertEquals(bb.limit(), exec("def bb = params['bb']; bb.limit()", params, true));
        assertEquals(bb.limit(), exec("CharBuffer bb = params['bb']; bb.limit()", params, true));

        assertEquals(bb.get(0), exec("def bb = params['bb']; bb.get(0)", params, true));
        assertEquals(bb.get(0), exec("CharBuffer bb = params['bb']; bb.get(0)", params, true));
        assertEquals(bb.get(1), exec("def bb = params['bb']; bb.get(1)", params, true));
        assertEquals(bb.get(1), exec("CharBuffer bb = params['bb']; bb.get(1)", params, true));
    }

    public void testDoubleBufferMethods() {
        DoubleBuffer bb = DoubleBuffer.wrap(new double[] { 0, 1, 2, 3, 4, 5, 6, 7 });
        Map<String, Object> params = Collections.singletonMap("bb", bb);

        assertEquals(bb.limit(), exec("def bb = params['bb']; bb.limit()", params, true));
        assertEquals(bb.limit(), exec("DoubleBuffer bb = params['bb']; bb.limit()", params, true));

        assertEquals(bb.get(0), exec("def bb = params['bb']; bb.get(0)", params, true));
        assertEquals(bb.get(0), exec("DoubleBuffer bb = params['bb']; bb.get(0)", params, true));
        assertEquals(bb.get(1), exec("def bb = params['bb']; bb.get(1)", params, true));
        assertEquals(bb.get(1), exec("DoubleBuffer bb = params['bb']; bb.get(1)", params, true));
    }

    public void testFloatBufferMethods() {
        FloatBuffer bb = FloatBuffer.wrap(new float[] { 0, 1, 2, 3, 4, 5, 6, 7 });
        Map<String, Object> params = Collections.singletonMap("bb", bb);

        assertEquals(bb.limit(), exec("def bb = params['bb']; bb.limit()", params, true));
        assertEquals(bb.limit(), exec("FloatBuffer bb = params['bb']; bb.limit()", params, true));

        assertEquals(bb.get(0), exec("def bb = params['bb']; bb.get(0)", params, true));
        assertEquals(bb.get(0), exec("FloatBuffer bb = params['bb']; bb.get(0)", params, true));
        assertEquals(bb.get(1), exec("def bb = params['bb']; bb.get(1)", params, true));
        assertEquals(bb.get(1), exec("FloatBuffer bb = params['bb']; bb.get(1)", params, true));
    }

    public void testIntBufferMethods() {
        IntBuffer bb = IntBuffer.wrap(new int[] { 0, 1, 2, 3, 4, 5, 6, 7 });
        Map<String, Object> params = Collections.singletonMap("bb", bb);

        assertEquals(bb.limit(), exec("def bb = params['bb']; bb.limit()", params, true));
        assertEquals(bb.limit(), exec("IntBuffer bb = params['bb']; bb.limit()", params, true));

        assertEquals(bb.get(0), exec("def bb = params['bb']; bb.get(0)", params, true));
        assertEquals(bb.get(0), exec("IntBuffer bb = params['bb']; bb.get(0)", params, true));
        assertEquals(bb.get(1), exec("def bb = params['bb']; bb.get(1)", params, true));
        assertEquals(bb.get(1), exec("IntBuffer bb = params['bb']; bb.get(1)", params, true));
    }

    public void testLongBufferMethods() {
        LongBuffer bb = LongBuffer.wrap(new long[] { 0, 1, 2, 3, 4, 5, 6, 7 });
        Map<String, Object> params = Collections.singletonMap("bb", bb);

        assertEquals(bb.limit(), exec("def bb = params['bb']; bb.limit()", params, true));
        assertEquals(bb.limit(), exec("LongBuffer bb = params['bb']; bb.limit()", params, true));

        assertEquals(bb.get(0), exec("def bb = params['bb']; bb.get(0)", params, true));
        assertEquals(bb.get(0), exec("LongBuffer bb = params['bb']; bb.get(0)", params, true));
        assertEquals(bb.get(1), exec("def bb = params['bb']; bb.get(1)", params, true));
        assertEquals(bb.get(1), exec("LongBuffer bb = params['bb']; bb.get(1)", params, true));
    }

    public void testShortBufferMethods() {
        ShortBuffer bb = ShortBuffer.wrap(new short[] { 0, 1, 2, 3, 4, 5, 6, 7 });
        Map<String, Object> params = Collections.singletonMap("bb", bb);

        assertEquals(bb.limit(), exec("def bb = params['bb']; bb.limit()", params, true));
        assertEquals(bb.limit(), exec("ShortBuffer bb = params['bb']; bb.limit()", params, true));

        assertEquals(bb.get(0), exec("def bb = params['bb']; bb.get(0)", params, true));
        assertEquals(bb.get(0), exec("ShortBuffer bb = params['bb']; bb.get(0)", params, true));
        assertEquals(bb.get(1), exec("def bb = params['bb']; bb.get(1)", params, true));
        assertEquals(bb.get(1), exec("ShortBuffer bb = params['bb']; bb.get(1)", params, true));
    }
}
