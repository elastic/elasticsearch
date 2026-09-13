/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.IllegalReferenceCountException;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.greaterThan;

public class NettyAllocatorTests extends ESTestCase {

    private static void assertBufferTrashed(String context, BytesReference bytesRef) throws IOException {
        var iter = bytesRef.iterator();
        BytesRef br;
        while ((br = iter.next()) != null) {
            for (var i = br.offset; i < br.offset + br.length; i++) {
                assertEquals(context + " off=" + br.offset + " len=" + br.length + " i=" + i, 0, br.bytes[i]);
            }
        }
    }

    private enum Structure {
        ROOT,
        RETAINED_SLICE,
        RETAINED_DUPLICATE,
        COMPOSITE
    }

    private enum Derivation {
        COPY,
        COPY_RANGE,
        READ_BYTES
    }

    /**
     * A randomly shaped buffer built out of roots, retained slices and composites, together with everything a test
     * needs to reason about it: the content it should hold, and every handle the test owns and must release.
     * {@code handles} is deliberately not the list of buffers that were created - a composite takes ownership of its
     * components, so those are excluded, while the parent of a retained slice is not, so those are included.
     */
    private record RandomBuf(ByteBuf buf, byte[] content, List<ByteBuf> handles, String description) {}

    private static RandomBuf randomBuf(TrashingByteBufAllocator alloc) {
        var handles = new ArrayList<ByteBuf>();
        var description = new StringBuilder();
        var buf = randomStructure(alloc, handles, description, 0);
        handles.add(buf);
        return new RandomBuf(buf, ByteBufUtil.getBytes(buf), handles, description.toString());
    }

    private static ByteBuf randomStructure(TrashingByteBufAllocator alloc, List<ByteBuf> handles, StringBuilder desc, int depth) {
        return switch (depth < 2 ? randomFrom(Structure.values()) : Structure.ROOT) {
            case ROOT -> randomRoot(alloc, desc);
            case RETAINED_SLICE -> {
                var parent = randomStructure(alloc, handles, desc, depth + 1);
                handles.add(parent);
                var offset = between(0, parent.readableBytes() - 1);
                var length = between(1, parent.readableBytes() - offset);
                desc.append(".slice(").append(offset).append(",").append(length).append(")");
                yield parent.retainedSlice(parent.readerIndex() + offset, length);
            }
            case RETAINED_DUPLICATE -> {
                var parent = randomStructure(alloc, handles, desc, depth + 1);
                handles.add(parent);
                desc.append(".duplicate()");
                yield parent.retainedDuplicate();
            }
            case COMPOSITE -> randomComposite(alloc, desc, depth);
        };
    }

    private static ByteBuf randomRoot(TrashingByteBufAllocator alloc, StringBuilder desc) {
        var size = between(64, 1024);
        desc.append("root(").append(size).append(")");
        return alloc.heapBuffer(size, size).writeBytes(randomByteArrayOfLength(size));
    }

    /**
     * Components are always buffers whose ownership is fully transferred to the composite, never views onto a buffer
     * the caller still holds: a composite trashes everything it spans when it is released, so a component that aliases
     * a live buffer would zero content that buffer's owner can still legitimately read.
     */
    private static ByteBuf randomComposite(TrashingByteBufAllocator alloc, StringBuilder desc, int depth) {
        var composite = alloc.compositeHeapBuffer();
        desc.append("composite(");
        for (var i = 0; i < between(1, 3); i++) {
            if (i > 0) {
                desc.append(", ");
            }
            composite.addComponent(true, depth < 2 && randomBoolean() ? randomComposite(alloc, desc, depth + 1) : randomRoot(alloc, desc));
        }
        desc.append(")");
        return composite;
    }

    private static ByteBuf derive(Derivation derivation, ByteBuf source) {
        var readable = source.readableBytes();
        return switch (derivation) {
            case COPY -> source.copy();
            case COPY_RANGE -> {
                var offset = between(0, readable - 1);
                yield source.copy(source.readerIndex() + offset, between(1, readable - offset));
            }
            case READ_BYTES -> source.readBytes(between(1, readable));
        };
    }

    public void testTrashingZeroesExactlyTheWrittenRegion() throws IOException {
        var alloc = new TrashingByteBufAllocator(ByteBufAllocator.DEFAULT);
        var size = between(64, 512);
        var before = alloc.heapBuffer(size, size).writeBytes(randomByteArrayOfLength(size));
        var victim = alloc.heapBuffer(size, size).writeBytes(randomByteArrayOfLength(size));
        var after = alloc.heapBuffer(size, size).writeBytes(randomByteArrayOfLength(size));

        assertSame("expected the pooled buffers to share a chunk array", victim.array(), before.array());
        assertSame("expected the pooled buffers to share a chunk array", victim.array(), after.array());
        assertEquals(
            "expected three distinct slots in the chunk, offsets="
                + before.arrayOffset()
                + ","
                + victim.arrayOffset()
                + ","
                + after.arrayOffset(),
            3,
            Set.of(before.arrayOffset(), victim.arrayOffset(), after.arrayOffset()).size()
        );

        var written = between(1, size - 1);
        victim.writerIndex(written);
        var victimRef = Netty4Utils.toBytesReference(victim);
        victim.skipBytes(between(1, written));

        var chunk = victim.array();
        var tailFrom = victim.arrayOffset() + written;
        var tailTo = victim.arrayOffset() + size;
        var tail = Arrays.copyOfRange(chunk, tailFrom, tailTo);
        var beforeContent = ByteBufUtil.getBytes(before);
        var afterContent = ByteBufUtil.getBytes(after);

        victim.release();

        assertBufferTrashed("written=" + written, victimRef);
        assertArrayEquals(
            "bytes past the writer index must not be trashed, written=" + written,
            tail,
            Arrays.copyOfRange(chunk, tailFrom, tailTo)
        );
        assertArrayEquals("preceding pooled buffer must not be trashed", beforeContent, ByteBufUtil.getBytes(before));
        assertArrayEquals("following pooled buffer must not be trashed", afterContent, ByteBufUtil.getBytes(after));

        before.release();
        after.release();
    }

    public void testContentSurvivesUntilLastReferenceReleased() throws IOException {
        var alloc = new TrashingByteBufAllocator(ByteBufAllocator.DEFAULT);
        var target = randomBuf(alloc);
        var ref = Netty4Utils.toBytesReference(target.buf());

        var handles = new ArrayList<>(target.handles());
        Collections.shuffle(handles, random());
        for (var i = 0; i < handles.size(); i++) {
            assertArrayEquals(
                "content must be intact with "
                    + (handles.size() - i)
                    + " of "
                    + handles.size()
                    + " handles outstanding, "
                    + target.description(),
                target.content(),
                BytesReference.toBytes(ref)
            );
            var handle = handles.get(i);
            handle.release();
            if (handle.refCnt() == 0) {
                expectThrows(IllegalReferenceCountException.class, () -> handle.getByte(handle.readerIndex()));
            }
        }

        handles.forEach(handle -> assertEquals(target.description(), 0, handle.refCnt()));
        assertBufferTrashed(target.description(), ref);
    }

    public void testEveryDerivedBufferIsTrashing() throws IOException {
        var alloc = new TrashingByteBufAllocator(ByteBufAllocator.DEFAULT);
        var derivation = randomFrom(Derivation.values());
        var target = randomBuf(alloc);
        var context = target.description() + " derived by " + derivation;

        var sourceRef = Netty4Utils.toBytesReference(target.buf());
        var derived = derive(derivation, target.buf());
        var derivedRef = Netty4Utils.toBytesReference(derived);
        assertThat(context, derivedRef.length(), greaterThan(0));

        derived.release();
        assertBufferTrashed(context, derivedRef);
        assertArrayEquals(
            "source must survive releasing a buffer derived from it, " + context,
            target.content(),
            BytesReference.toBytes(sourceRef)
        );

        target.handles().forEach(ByteBuf::release);
    }

    public void testGrowthPreservesContiguityAndContent() throws IOException {
        var alloc = new TrashingByteBufAllocator(ByteBufAllocator.DEFAULT);
        var initialCapacity = between(1, 64);
        var buf = alloc.heapBuffer(initialCapacity);
        var content = randomByteArrayOfLength(between(16 * 1024, 128 * 1024));
        buf.writeBytes(content);

        assertTrue("a grown buffer must stay backed by a single array", buf.hasArray());
        var ref = Netty4Utils.toBytesReference(buf);
        assertArrayEquals(content, BytesReference.toBytes(ref));

        buf.release();
        assertBufferTrashed("grown from " + initialCapacity + " to " + content.length, ref);
    }

    public void testConcurrentReleaseTrashesExactlyOnce() throws IOException {
        var alloc = new TrashingByteBufAllocator(ByteBufAllocator.DEFAULT);
        var target = randomBuf(alloc);
        var ref = Netty4Utils.toBytesReference(target.buf());

        var readers = between(2, 8);
        var slices = new ArrayList<ByteBuf>();
        for (var i = 0; i < readers; i++) {
            slices.add(target.buf().retainedSlice());
        }

        startInParallel(readers + 1, task -> {
            if (task == readers) {
                target.handles().forEach(ByteBuf::release);
                return;
            }
            var slice = slices.get(task);
            try {
                for (var round = 0; round < 32; round++) {
                    assertArrayEquals(
                        "a slice must not be trashed while it holds a reference, " + target.description(),
                        target.content(),
                        ByteBufUtil.getBytes(slice)
                    );
                }
            } finally {
                slice.release();
            }
        });

        assertBufferTrashed(target.description(), ref);
    }
}
