/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;

/**
 * Tests for {@link BytesRefArrowBlock#ofSanitizedUtf8}, which repairs malformed UTF-8 in Arrow
 * {@code VARCHAR} vectors while keeping the well-formed common case zero-copy.
 */
public class BytesRefArrowSanitizeTests extends ESTestCase {

    private RootAllocator allocator;
    private BlockFactory blockFactory;

    @Before
    public void setup() {
        allocator = new RootAllocator();
        blockFactory = new BlockFactory(new NoopCircuitBreaker("test-noop"), BigArrays.NON_RECYCLING_INSTANCE);
    }

    @After
    public void cleanup() {
        allocator.close();
    }

    public void testWellFormedFlatStaysZeroCopy() {
        try (VarCharVector arrowVec = new VarCharVector("test", allocator)) {
            arrowVec.allocateNew();
            arrowVec.set(0, "hello".getBytes(StandardCharsets.UTF_8));
            arrowVec.set(1, "caf\u00e9".getBytes(StandardCharsets.UTF_8));
            arrowVec.set(2, "\ud83d\ude00".getBytes(StandardCharsets.UTF_8));
            arrowVec.setValueCount(3);

            try (Block block = BytesRefArrowBlock.ofSanitizedUtf8(arrowVec, blockFactory)) {
                assertTrue("well-formed VARCHAR must stay zero-copy", block instanceof BytesRefArrowBufBlock);
                BytesRefBlock refs = (BytesRefBlock) block;
                BytesRef scratch = new BytesRef();
                assertEquals("hello", refs.getBytesRef(0, scratch).utf8ToString());
                assertEquals("caf\u00e9", refs.getBytesRef(1, scratch).utf8ToString());
                assertEquals("\ud83d\ude00", refs.getBytesRef(2, scratch).utf8ToString());
            }
        }
    }

    public void testMalformedFlatIsSanitized() {
        try (VarCharVector arrowVec = new VarCharVector("test", allocator)) {
            arrowVec.allocateNew();
            arrowVec.set(0, new byte[] { (byte) 0xFF });               // lone invalid lead byte
            arrowVec.setNull(1);
            arrowVec.set(2, "ok".getBytes(StandardCharsets.UTF_8));
            arrowVec.set(3, new byte[] { (byte) 0xC3, (byte) 0x28 });  // truncated 2-byte + '('
            arrowVec.setValueCount(4);

            try (Block block = BytesRefArrowBlock.ofSanitizedUtf8(arrowVec, blockFactory)) {
                assertFalse("malformed VARCHAR must be rebuilt", block instanceof BytesRefArrowBufBlock);
                BytesRefBlock refs = (BytesRefBlock) block;
                BytesRef scratch = new BytesRef();
                assertEquals(new BytesRef("\uFFFD"), refs.getBytesRef(refs.getFirstValueIndex(0), scratch));
                assertTrue(refs.isNull(1));
                assertEquals(new BytesRef("ok"), refs.getBytesRef(refs.getFirstValueIndex(2), scratch));
                assertEquals(new BytesRef("\uFFFD("), refs.getBytesRef(refs.getFirstValueIndex(3), scratch));
            }
        }
    }

    public void testMalformedListIsSanitized() {
        // Position 0: ["aa", <0xFF>], Position 1: ["ok"]; no null children (matches the zero-copy list contract).
        try (ListVector listVector = ListVector.empty("test", allocator)) {
            listVector.addOrGetVector(FieldType.nullable(Types.MinorType.VARCHAR.getType()));
            listVector.allocateNew();
            VarCharVector child = (VarCharVector) listVector.getDataVector();
            child.allocateNew();
            child.set(0, "aa".getBytes(StandardCharsets.UTF_8));
            child.set(1, new byte[] { (byte) 0xFF });
            child.set(2, "ok".getBytes(StandardCharsets.UTF_8));
            child.setValueCount(3);

            ArrowBuf offsets = listVector.getOffsetBuffer();
            offsets.setInt(0, 0);
            offsets.setInt(4, 2);
            offsets.setInt(8, 3);

            ArrowBuf validity = listVector.getValidityBuffer();
            validity.setZero(0, validity.capacity());
            BitVectorHelper.setBit(validity, 0);
            BitVectorHelper.setBit(validity, 1);
            listVector.setLastSet(1);
            listVector.setValueCount(2);

            try (Block block = BytesRefArrowBlock.ofSanitizedUtf8(listVector, blockFactory)) {
                BytesRefBlock refs = (BytesRefBlock) block;
                BytesRef scratch = new BytesRef();
                assertEquals(2, refs.getValueCount(0));
                int start = refs.getFirstValueIndex(0);
                assertEquals(new BytesRef("aa"), refs.getBytesRef(start, scratch));
                assertEquals(new BytesRef("\uFFFD"), refs.getBytesRef(start + 1, scratch));
                assertEquals(1, refs.getValueCount(1));
                assertEquals(new BytesRef("ok"), refs.getBytesRef(refs.getFirstValueIndex(1), scratch));
            }
        }
    }

    public void testMalformedListWithChildNullsFollowsArrowListSupport() {
        // Position 0: ["aa", <null>, <0xFF>] -> null child dropped, malformed repaired -> ["aa", U+FFFD].
        // Position 1: [<null>] -> all children null -> collapses to an ESQL null position (ofList semantics).
        try (ListVector listVector = ListVector.empty("test", allocator)) {
            listVector.addOrGetVector(FieldType.nullable(Types.MinorType.VARCHAR.getType()));
            listVector.allocateNew();
            VarCharVector child = (VarCharVector) listVector.getDataVector();
            child.allocateNew();
            child.set(0, "aa".getBytes(StandardCharsets.UTF_8));
            child.setNull(1);
            child.set(2, new byte[] { (byte) 0xFF });
            child.setNull(3);
            child.setValueCount(4);

            ArrowBuf offsets = listVector.getOffsetBuffer();
            offsets.setInt(0, 0);
            offsets.setInt(4, 3);
            offsets.setInt(8, 4);

            ArrowBuf validity = listVector.getValidityBuffer();
            validity.setZero(0, validity.capacity());
            BitVectorHelper.setBit(validity, 0);
            BitVectorHelper.setBit(validity, 1);
            listVector.setLastSet(1);
            listVector.setValueCount(2);

            try (Block block = BytesRefArrowBlock.ofSanitizedUtf8(listVector, blockFactory)) {
                BytesRefBlock refs = (BytesRefBlock) block;
                BytesRef scratch = new BytesRef();
                assertEquals(2, refs.getValueCount(0));
                int start = refs.getFirstValueIndex(0);
                assertEquals(new BytesRef("aa"), refs.getBytesRef(start, scratch));
                assertEquals(new BytesRef("\uFFFD"), refs.getBytesRef(start + 1, scratch));
                assertTrue("all-null child list must collapse to null", refs.isNull(1));
            }
        }
    }
}
