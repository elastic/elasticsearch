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

    private static int TEST_ARRAY_SIZE = 128;
    private static byte[] testArray;

    @BeforeClass
    static void createTestArray() throws Exception {
        testArray = new byte[TEST_ARRAY_SIZE];
        for (int i = 0; i < testArray.length; i++) {
            testArray[i] = (byte) i;
        }
    }

    public void testSimpleMarkResetAtBeginning() throws Exception {
        for (int length = 1; length <= 8; length++) {
            for (int mark = 1; mark <= length; mark++) {
                try (BufferOnMarkInputStream in = new BufferOnMarkInputStream(new NoMarkByteArrayInputStream(testArray, 0, length), mark)) {
                    in.mark(mark);
                    byte[] test1 = in.readNBytes(mark);
                    assertArray(0, test1);
                    in.reset();
                    byte[] test2 = in.readNBytes(mark);
                    assertArray(0, test2);
                }
            }
        }
    }

    public void testMarkResetAtBeginning() throws Exception {
        for (int length = 1; length <= 8; length++) {
            try (BufferOnMarkInputStream in = new BufferOnMarkInputStream(new NoMarkByteArrayInputStream(testArray, 0, length), length)) {
                in.mark(length);
                for (int readLen = 1; readLen <= length; readLen++) {
                    byte[] test1 = in.readNBytes(readLen);
                    assertArray(0, test1);
                    in.reset();
                }
            }
        }
    }

    public void testSimpleMarkResetEverywhere() throws Exception {
        for (int length = 1; length <= 8; length++) {
            for (int offset = 0; offset < length; offset++) {
                for (int mark = 1; mark <= length - offset; mark++) {
                    try (BufferOnMarkInputStream in = new BufferOnMarkInputStream(new NoMarkByteArrayInputStream(testArray, 0, length), mark)) {
                        // skip first offset bytes
                        in.readNBytes(offset);
                        in.mark(mark);
                        byte[] test1 = in.readNBytes(mark);
                        assertArray(offset, test1);
                        in.reset();
                        byte[] test2 = in.readNBytes(mark);
                        assertArray(offset, test2);
                    }
                }
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
