package org.elasticsearch.common.io.stream;

import org.elasticsearch.common.bytes.ByteBufferBytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class StreamTests extends ESTestCase {
    public void testRandomVLongSerialization() throws IOException {
        for (int i = 0; i < 1024; i++) {
            long write = randomLong();
            BytesStreamOutput out = new BytesStreamOutput();
            out.writeZLong(write);
            long read = out.bytes().streamInput().readZLong();
            assertEquals(write, read);
        }
    }

    public void testSpecificVLongSerialization() throws IOException {
        List<Tuple<Long, byte[]>> values =
                Arrays.asList(
                        new Tuple<>(0L, new byte[]{0}),
                        new Tuple<>(-1L, new byte[]{1}),
                        new Tuple<>(1L, new byte[]{2}),
                        new Tuple<>(-2L, new byte[]{3}),
                        new Tuple<>(2L, new byte[]{4}),
                        new Tuple<>(Long.MIN_VALUE, new byte[]{-1, -1, -1, -1, -1, -1, -1, -1, -1, 1}),
                        new Tuple<>(Long.MAX_VALUE, new byte[]{-2, -1, -1, -1, -1, -1, -1, -1, -1, 1})

                );
        for (Tuple<Long, byte[]> value : values) {
            BytesStreamOutput out = new BytesStreamOutput();
            out.writeZLong(value.v1());
            assertArrayEquals(Long.toString(value.v1()), value.v2(), out.bytes().toBytes());
            ByteBufferBytesReference bytes = new ByteBufferBytesReference(ByteBuffer.wrap(value.v2()));
            assertEquals(Arrays.toString(value.v2()), (long)value.v1(), bytes.streamInput().readZLong());
        }
    }
}
