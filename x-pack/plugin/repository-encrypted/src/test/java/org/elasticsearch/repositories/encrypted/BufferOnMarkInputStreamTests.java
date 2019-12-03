package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.BeforeClass;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Arrays;

public class BufferOnMarkInputStreamTests extends ESTestCase {

    private static byte[] testArray;

    @BeforeClass
    static void createTestArray() throws Exception {
        testArray = new byte[128];
        for (int i = 0; i < testArray.length; i++) {
            testArray[i] = (byte) i;
        }
    }

    public void testSimpleMarkResetAtBeginning() throws Exception {
        for (int length = 1; length <= 16; length++) {
            for (int mark = 1; mark <= length; mark++) {
                try (BufferOnMarkInputStream in = new BufferOnMarkInputStream(new NoMarkByteArrayInputStream(testArray, 0, length), mark)) {
                    in.mark(mark);
                    assertThat(in.getCurrentBufferCount(), Matchers.is(0));
                    assertThat(in.getRemainingBufferCapacity(), Matchers.is(mark));
                    byte[] test1 = in.readNBytes(mark);
                    assertArray(0, test1);
                    assertThat(in.getCurrentBufferCount(), Matchers.is(mark));
                    assertThat(in.getRemainingBufferCapacity(), Matchers.is(0));
                    in.reset();
                    assertThat(in.getCurrentBufferCount(), Matchers.is(mark));
                    assertThat(in.getRemainingBufferCapacity(), Matchers.is(0));
                    byte[] test2 = in.readNBytes(mark);
                    assertArray(0, test2);
                    assertThat(in.getCurrentBufferCount(), Matchers.is(mark));
                    assertThat(in.getRemainingBufferCapacity(), Matchers.is(0));
                }
            }
        }
    }

    public void testMarkResetAtBeginning() throws Exception {
        for (int length = 1; length <= 16; length++) {
            try (BufferOnMarkInputStream in = new BufferOnMarkInputStream(new NoMarkByteArrayInputStream(testArray, 0, length), length)) {
                in.mark(length);
                // increasing length read/reset
                for (int readLen = 1; readLen <= length; readLen++) {
                    byte[] test1 = in.readNBytes(readLen);
                    assertArray(0, test1);
                    in.reset();
                }
            }
            try (BufferOnMarkInputStream in = new BufferOnMarkInputStream(new NoMarkByteArrayInputStream(testArray, 0, length), length)) {
                in.mark(length);
                // decreasing length read/reset
                for (int readLen = length; readLen >= 1; readLen--) {
                    byte[] test1 = in.readNBytes(readLen);
                    assertArray(0, test1);
                    in.reset();
                }
            }
        }
    }

    public void testSimpleMarkResetEverywhere() throws Exception {
        for (int length = 1; length <= 16; length++) {
            for (int offset = 0; offset < length; offset++) {
                for (int mark = 1; mark <= length - offset; mark++) {
                    try (BufferOnMarkInputStream in = new BufferOnMarkInputStream(new NoMarkByteArrayInputStream(testArray, 0, length), mark)) {
                        // skip first offset bytes
                        in.readNBytes(offset);
                        in.mark(mark);
                        assertThat(in.getCurrentBufferCount(), Matchers.is(0));
                        assertThat(in.getRemainingBufferCapacity(), Matchers.is(mark));
                        byte[] test1 = in.readNBytes(mark);
                        assertArray(offset, test1);
                        assertThat(in.getCurrentBufferCount(), Matchers.is(mark));
                        assertThat(in.getRemainingBufferCapacity(), Matchers.is(0));
                        in.reset();
                        assertThat(in.getCurrentBufferCount(), Matchers.is(mark));
                        assertThat(in.getRemainingBufferCapacity(), Matchers.is(0));
                        byte[] test2 = in.readNBytes(mark);
                        assertThat(in.getCurrentBufferCount(), Matchers.is(mark));
                        assertThat(in.getRemainingBufferCapacity(), Matchers.is(0));
                        assertArray(offset, test2);
                    }
                }
            }
        }
    }

    public void testMarkResetEverywhere() throws Exception {
        for (int length = 1; length <= 16; length++) {
            for (int offset = 0; offset < length; offset++) {
                try (BufferOnMarkInputStream in = new BufferOnMarkInputStream(new NoMarkByteArrayInputStream(testArray, 0, length),
                        length)) {
                    // skip first offset bytes
                    in.readNBytes(offset);
                    in.mark(length);
                    // increasing read lengths
                    for (int readLen = 1; readLen <= length - offset; readLen++) {
                        byte[] test = in.readNBytes(readLen);
                        assertArray(offset, test);
                        in.reset();
                    }
                }
                try (BufferOnMarkInputStream in = new BufferOnMarkInputStream(new NoMarkByteArrayInputStream(testArray, 0, length),
                        length)) {
                    // skip first offset bytes
                    in.readNBytes(offset);
                    in.mark(length);
                    // decreasing read lengths
                    for (int readLen = length - offset; readLen >= 1; readLen--) {
                        byte[] test = in.readNBytes(readLen);
                        assertArray(offset, test);
                        in.reset();
                    }
                }
            }
        }
    }

    public void testDoubleMarkEverywhere() throws Exception {
        for (int length = 1; length <= 16; length++) {
            for (int offset = 0; offset < length; offset++) {
                for (int readLen = 1; readLen <= length - offset; readLen++) {
                    for (int markLen = 1; markLen <= length - offset; markLen++) {
                        try (BufferOnMarkInputStream in = new BufferOnMarkInputStream(new NoMarkByteArrayInputStream(testArray, 0, length),
                                length)) {
                            in.readNBytes(offset);
                            assertThat(in.getCurrentBufferCount(), Matchers.is(0));
                            assertThat(in.getRemainingBufferCapacity(), Matchers.is(0));
                            assertThat(in.getRemainingBufferToRead(), Matchers.is(0));
                            // first mark
                            in.mark(length - offset);
                            assertThat(in.getCurrentBufferCount(), Matchers.is(0));
                            assertThat(in.getRemainingBufferCapacity(), Matchers.is(length));
                            assertThat(in.getRemainingBufferToRead(), Matchers.is(0));
                            byte[] test = in.readNBytes(readLen);
                            assertArray(offset, test);
                            assertThat(in.getCurrentBufferCount(), Matchers.is(readLen));
                            assertThat(in.getRemainingBufferCapacity(), Matchers.is(length - readLen));
                            assertThat(in.getRemainingBufferToRead(), Matchers.is(readLen));
                            // reset to first
                            in.reset();
                            assertThat(in.getCurrentBufferCount(), Matchers.is(readLen));
                            assertThat(in.getRemainingBufferCapacity(), Matchers.is(length - readLen));
                            assertThat(in.getRemainingBufferToRead(), Matchers.is(readLen));
                            // advance before/after the first read length
                            test = in.readNBytes(markLen);
                            assertThat(in.getCurrentBufferCount(), Matchers.is(Math.max(readLen, markLen)));
                            assertThat(in.getRemainingBufferCapacity(), Matchers.is(length - Math.max(readLen, markLen)));
                            if (markLen <= readLen) {
                                assertThat(in.getRemainingBufferToRead(), Matchers.is(readLen - markLen));
                            } else {
                                assertThat(in.resetCalled, Matchers.is(false));
                            }
                            assertArray(offset, test);
                            // second mark
                            in.mark(length - offset - markLen);
                            if (markLen <= readLen) {
                                assertThat(in.getCurrentBufferCount(), Matchers.is(readLen - markLen));
                                assertThat(in.getRemainingBufferCapacity(), Matchers.is(length - readLen + markLen));
                                assertThat(in.getRemainingBufferToRead(), Matchers.is(readLen - markLen));
                            } else {
                                assertThat(in.getCurrentBufferCount(), Matchers.is(0));
                                assertThat(in.getRemainingBufferCapacity(), Matchers.is(length));
                                assertThat(in.getRemainingBufferToRead(), Matchers.is(0));
                            }
                            for (int readLen2 = 1; readLen2 <= length - offset - markLen; readLen2++) {
                                byte[] test2 = in.readNBytes(readLen2);
                                if (markLen + readLen2 <= readLen) {
                                    assertThat(in.getCurrentBufferCount(), Matchers.is(readLen - markLen));
                                    assertThat(in.getRemainingBufferCapacity(), Matchers.is(length - readLen + markLen));
                                    assertThat(in.resetCalled, Matchers.is(true));
                                    assertThat(in.getRemainingBufferToRead(), Matchers.is(readLen - markLen - readLen2));
                                } else {
                                    assertThat(in.getCurrentBufferCount(), Matchers.is(readLen2));
                                    assertThat(in.getRemainingBufferCapacity(), Matchers.is(length - readLen2));
                                    assertThat(in.resetCalled, Matchers.is(false));
                                }
                                assertArray(offset + markLen, test2);
                                in.reset();
                                assertThat(in.resetCalled, Matchers.is(true));
                                if (markLen + readLen2 <= readLen) {
                                    assertThat(in.getCurrentBufferCount(), Matchers.is(readLen - markLen));
                                    assertThat(in.getRemainingBufferCapacity(), Matchers.is(length - readLen + markLen));
                                    assertThat(in.getRemainingBufferToRead(), Matchers.is(readLen - markLen));
                                } else {
                                    assertThat(in.getCurrentBufferCount(), Matchers.is(readLen2));
                                    assertThat(in.getRemainingBufferCapacity(), Matchers.is(length - readLen2));
                                    assertThat(in.getRemainingBufferToRead(), Matchers.is(readLen2));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public void testMarkWithoutReset() throws Exception {
        int maxMark = 8;
        BufferOnMarkInputStream in = new BufferOnMarkInputStream(new NoMarkByteArrayInputStream(testArray, 0, testArray.length), maxMark);
        int offset = 0;
        while (offset < testArray.length) {
            int readLen = Math.min(1 + Randomness.get().nextInt(maxMark), testArray.length - offset);
            in.mark(Randomness.get().nextInt(readLen));
            assertThat(in.getCurrentBufferCount(), Matchers.is(0));
            assertThat(in.getRemainingBufferCapacity(), Matchers.is(maxMark));
            assertThat(in.getRemainingBufferToRead(), Matchers.is(0));
            byte[] test = in.readNBytes(readLen);
            assertThat(in.getCurrentBufferCount(), Matchers.is(readLen));
            assertThat(in.getRemainingBufferCapacity(), Matchers.is(maxMark - readLen));
            assertThat(in.getRemainingBufferToRead(), Matchers.is(readLen));
            assertArray(offset, test);
            offset += readLen;
        }
    }

    public void testThreeMarkResetMarkSteps() throws Exception {
        int length = 8 + Randomness.get().nextInt(8);
        int stepLen = 4 + Randomness.get().nextInt(4);
        BufferOnMarkInputStream in = new BufferOnMarkInputStream(new NoMarkByteArrayInputStream(testArray, 0, length), stepLen);
        testMarkResetMarkStep(in, 0, length, stepLen, 2);
    }

    private void testMarkResetMarkStep(BufferOnMarkInputStream stream, int offset, int length, int stepLen, int step) throws Exception {
        stream.mark(stepLen);
        for (int readLen = 1; readLen <= Math.min(stepLen, length - offset); readLen++) {
            for (int markLen = 1; markLen <= Math.min(stepLen, length - offset); markLen++) {
                // read ahead
                byte[] test = stream.readNBytes(readLen);
                assertArray(offset, test);
                // reset back
                stream.reset();
                // read ahead different length
                test = stream.readNBytes(markLen);
                assertArray(offset, test);
                if (step > 0) {
                    int nextStepOffset = ((NoMarkByteArrayInputStream) stream.getWrapped()).getPos();
                    BufferOnMarkInputStream cloneStream = new BufferOnMarkInputStream(new NoMarkByteArrayInputStream(testArray,
                            nextStepOffset, length - nextStepOffset), stepLen);
                    if (stream.ringBuffer != null) {
                        cloneStream.ringBuffer = Arrays.copyOf(stream.ringBuffer, stream.ringBuffer.length);
                    } else {
                        cloneStream.ringBuffer = null;
                    }
                    cloneStream.head = stream.head;
                    cloneStream.tail = stream.tail;
                    cloneStream.position = stream.position;
                    cloneStream.markCalled = stream.markCalled;
                    cloneStream.resetCalled = stream.resetCalled;
                    cloneStream.closed = stream.closed;
                    testMarkResetMarkStep(cloneStream, offset + markLen, length, stepLen, step - 1);
                }
                // reset back
                stream.reset();
            }
        }
    }

    private void assertArray(int offset, byte[] test) {
        for (int i = 0; i < test.length; i++) {
            Assert.assertThat(test[i], Matchers.is(testArray[offset + i]));
        }
    }

    static class NoMarkByteArrayInputStream extends ByteArrayInputStream {

        public NoMarkByteArrayInputStream(byte[] buf) {
            super(buf);
        }

        public NoMarkByteArrayInputStream(byte[] buf, int offset, int length) {
            super(buf, offset, length);
        }

        int getPos() {
            return pos;
        }

        @Override
        public void mark(int readlimit) {
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        @Override
        public void reset() {
            throw new IllegalStateException("Mark not called or has been invalidated");
        }
    }

}
