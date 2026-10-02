/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.lucene;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.elasticsearch.blobcache.common.BlobCacheBufferedIndexInput;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.DirectAccessInput;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.lucene.store.IndexInputUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.test.FakeStatelessNode;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;

import static org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils.pageAligned;
import static org.elasticsearch.xpack.stateless.commits.BlobLocationTestUtils.createBlobFileRanges;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/**
 * Tests the {@link DirectAccessInput} implementation of {@link IndexDirectory.ReopeningIndexInput} over both delegate states: a local
 * file on disk and, once the file has been uploaded, the shared blob cache.
 * <p>
 * Files named {@code .vec} are memory mapped by {@code HybridDirectory} and files named {@code .fdt} are not, which is how these tests
 * pick between an mmap and a plain local delegate.
 */
public class ReopeningIndexInputDirectAccessTests extends ESTestCase {

    private static final long PRIMARY_TERM = 1L;
    private static final long GENERATION = 1L;

    private static final String MMAP_FILE = "file.vec";
    private static final String PLAIN_FILE = "file.fdt";

    private static final int FILE_LENGTH = 16 * 1024;
    /** Big enough that {@link IndexDirectory.ReopeningIndexInput#withMemorySegmentSlice} serves it directly rather than leaving it to the
     * buffer, so that the tests below exercise the zero-copy path. {@link #testSmallReadIsLeftToTheBuffer} covers the other side. */
    private static final int RANGE_LENGTH = BlobCacheBufferedIndexInput.BUFFER_SIZE;
    private static final int SMALL_RANGE_LENGTH = 64;

    private static final long[] RANGE_OFFSETS = new long[] { 0, RANGE_LENGTH, 4L * RANGE_LENGTH, FILE_LENGTH - RANGE_LENGTH };

    public void testZeroCopyFromLocalMmapFile() throws Exception {
        try (var node = newNode()) {
            var bytes = writeFile(node, MMAP_FILE);
            try (var input = openReopeningInput(node, MMAP_FILE)) {
                assertThat(input.getDelegate().isCached(), equalTo(false));
                assertThat(input.getDelegate().getDelegate(), instanceOf(MemorySegmentAccessInput.class));

                assertSegmentSlice(input, bytes, 0, FILE_LENGTH);
                assertSegmentSlice(input, bytes, RANGE_LENGTH, RANGE_LENGTH);
                assertSliceAddresses(input, bytes, RANGE_OFFSETS);
            }
        }
    }

    public void testZeroCopyFromBlobCacheAfterUpload() throws Exception {
        try (var node = newNode()) {
            var bytes = writeFile(node, MMAP_FILE);
            try (var input = openReopeningInput(node, MMAP_FILE)) {
                upload(node, MMAP_FILE, bytes);
                // reads the whole file through the reopened input, which both flips the delegate and populates the cache region that
                // the memory segments are carved out of
                readFully(input, bytes);
                assertThat(input.getDelegate().isCached(), equalTo(true));

                assertSegmentSlice(input, bytes, 0, FILE_LENGTH);
                assertSegmentSlice(input, bytes, RANGE_LENGTH, RANGE_LENGTH);
                assertSliceAddresses(input, bytes, RANGE_OFFSETS);
            }
        }
    }

    /**
     * A zero-copy read holds a reference on the local file only for the duration of the action. If it kept one, the file could not be
     * deleted once uploaded, and the input could not be reopened from the cache.
     */
    public void testZeroCopyOnLocalFileDoesNotPinItAcrossUpload() throws Exception {
        try (var node = newNode()) {
            var bytes = writeFile(node, MMAP_FILE);
            try (var input = openReopeningInput(node, MMAP_FILE)) {
                assertSegmentSlice(input, bytes, 0, FILE_LENGTH);
                assertSliceAddresses(input, bytes, RANGE_OFFSETS);
                assertThat(input.getDelegate().isCached(), equalTo(false));
                assertThat(Files.exists(localIndexPath(node).resolve(MMAP_FILE)), equalTo(true));

                upload(node, MMAP_FILE, bytes);
                readFully(input, bytes);

                assertThat(Files.exists(localIndexPath(node).resolve(MMAP_FILE)), equalTo(false));
                assertThat(input.getDelegate().isCached(), equalTo(true));
                assertSegmentSlice(input, bytes, 0, FILE_LENGTH);
                assertSliceAddresses(input, bytes, RANGE_OFFSETS);
            }
        }
    }

    /**
     * A single read smaller than the buffer is declined, so that {@link IndexInputUtils#withSlice} leaves it on the buffered path.
     */
    public void testSmallReadIsLeftToTheBuffer() throws Exception {
        try (var node = newNode()) {
            var bytes = writeFile(node, MMAP_FILE);
            try (var input = openReopeningInput(node, MMAP_FILE)) {
                assertThat(
                    "the read has to be smaller than the buffer for this test to cover the branch it is about",
                    SMALL_RANGE_LENGTH,
                    lessThan(input.getBufferSize())
                );
                assertThat(input.getDelegate().getDelegate(), instanceOf(MemorySegmentAccessInput.class));

                // declined despite the delegate being able to serve it
                assertNoSegmentSlice(input, SMALL_RANGE_LENGTH);

                // ... and the caller still gets the right bytes, via the heap-copy fallback
                var scratch = new RecordingScratch();
                input.seek(0L);
                assertArrayEquals(
                    Arrays.copyOfRange(bytes, 0, SMALL_RANGE_LENGTH),
                    IndexInputUtils.withSlice(input, SMALL_RANGE_LENGTH, scratch, ReopeningIndexInputDirectAccessTests::toByteArray)
                );
                assertThat("the buffered fallback should have been used", scratch.used.get(), equalTo(true));

                // the bulk gather is unaffected by the size of the ranges
                var offsets = new long[] { 0, SMALL_RANGE_LENGTH, 2L * SMALL_RANGE_LENGTH };
                var invoked = new AtomicBoolean();
                var available = input.withSliceAddresses(
                    offsets,
                    SMALL_RANGE_LENGTH,
                    offsets.length,
                    addressesScratch(offsets.length),
                    addresses -> invoked.set(true)
                );
                assertThat("withSliceAddresses must not be gated on the buffer size", available, equalTo(true));
                assertThat(invoked.get(), equalTo(true));
            }
        }
    }

    public void testPlainLocalDelegate() throws Exception {
        try (var node = newNode()) {
            writeFile(node, PLAIN_FILE);
            try (var input = openReopeningInput(node, PLAIN_FILE)) {
                assertThat(input.getDelegate().getDelegate(), not(instanceOf(MemorySegmentAccessInput.class)));

                assertNoSegmentSlice(input, FILE_LENGTH);
                assertNoSliceAddresses(input);
            }
        }
    }

    public void testSlicesAndClones() throws Exception {
        try (var node = newNode()) {
            var bytes = writeFile(node, MMAP_FILE);
            try (var input = openReopeningInput(node, MMAP_FILE)) {
                var sliceOffset = RANGE_LENGTH;
                var sliceLength = FILE_LENGTH - 2 * RANGE_LENGTH;

                var slice = input.slice("slice", sliceOffset, sliceLength);
                assertThat(slice, instanceOf(IndexDirectory.ReopeningIndexInput.class));
                var sliceBytes = Arrays.copyOfRange(bytes, sliceOffset, sliceOffset + sliceLength);
                assertSegmentSlice((DirectAccessInput) slice, sliceBytes, 0, sliceLength);
                assertSegmentSlice((DirectAccessInput) slice, sliceBytes, RANGE_LENGTH, RANGE_LENGTH);
                assertSliceAddresses((DirectAccessInput) slice, sliceBytes, new long[] { 0, RANGE_LENGTH, 2 * RANGE_LENGTH });

                var clone = input.clone();
                assertThat(clone, instanceOf(IndexDirectory.ReopeningIndexInput.class));
                assertSegmentSlice((DirectAccessInput) clone, bytes, RANGE_LENGTH, RANGE_LENGTH);
                assertSliceAddresses((DirectAccessInput) clone, bytes, RANGE_OFFSETS);
            }
        }
    }

    public void testSliceAddressesArgumentValidation() throws Exception {
        try (var node = newNode()) {
            writeFile(node, MMAP_FILE);
            try (var input = openReopeningInput(node, MMAP_FILE)) {
                var offsets = RANGE_OFFSETS;

                var notInvoked = new AtomicBoolean();
                assertThat(
                    input.withSliceAddresses(offsets, RANGE_LENGTH, 0, addressesScratch(1), addresses -> notInvoked.set(true)),
                    equalTo(false)
                );
                assertThat("action must not be invoked for an empty request", notInvoked.get(), equalTo(false));

                expectThrows(
                    IllegalArgumentException.class,
                    () -> input.withSliceAddresses(offsets, RANGE_LENGTH, -1, addressesScratch(1), addresses -> {})
                );
                expectThrows(
                    IllegalArgumentException.class,
                    () -> input.withSliceAddresses(offsets, RANGE_LENGTH, offsets.length + 1, addressesScratch(1), addresses -> {})
                );
                expectThrows(
                    IllegalArgumentException.class,
                    () -> input.withSliceAddresses(offsets, RANGE_LENGTH, offsets.length, addressesScratch(1), addresses -> {})
                );
                expectThrows(
                    IllegalArgumentException.class,
                    () -> input.withSliceAddresses(
                        offsets,
                        RANGE_LENGTH,
                        offsets.length,
                        misalignedScratch(offsets.length),
                        addresses -> {}
                    )
                );
            }
        }
    }

    /**
     * Reads through the whole production wrapper chain that {@link org.elasticsearch.index.store.Store} builds, which is where the
     * zero-copy access has to survive in order to reach the vector scorers, and checks that the heap-copy fallback is taken for a plain
     * delegate and skipped for an mmap one.
     */
    public void testZeroCopyThroughStoreDirectory() throws Exception {
        try (var node = newNode()) {
            var mmapBytes = writeFile(node, MMAP_FILE);
            var plainBytes = writeFile(node, PLAIN_FILE);
            var directory = node.indexingStore.directory();

            try (var input = directory.openInput(MMAP_FILE, IOContext.DEFAULT)) {
                assertThat(IndexInputUtils.canUseSegmentSlices(input), equalTo(true));

                var scratch = new RecordingScratch();
                assertArrayEquals(
                    Arrays.copyOfRange(mmapBytes, 0, RANGE_LENGTH),
                    IndexInputUtils.withSlice(input, RANGE_LENGTH, scratch, ReopeningIndexInputDirectAccessTests::toByteArray)
                );
                assertThat("heap-copy fallback was taken for an mmap delegate", scratch.used.get(), equalTo(false));

                assertSliceAddressesThroughUtils(input, mmapBytes, true);
            }

            try (var input = directory.openInput(PLAIN_FILE, IOContext.DEFAULT)) {
                var scratch = new RecordingScratch();
                assertArrayEquals(
                    Arrays.copyOfRange(plainBytes, 0, RANGE_LENGTH),
                    IndexInputUtils.withSlice(input, RANGE_LENGTH, scratch, ReopeningIndexInputDirectAccessTests::toByteArray)
                );
                assertThat("heap-copy fallback should be taken for a plain delegate", scratch.used.get(), equalTo(true));

                assertSliceAddressesThroughUtils(input, plainBytes, false);
            }
        }
    }

    /** Records whether the heap-copy fallback of {@link IndexInputUtils#withSlice} was reached. */
    private static class RecordingScratch implements IntFunction<byte[]> {

        private final AtomicBoolean used = new AtomicBoolean();

        @Override
        public byte[] apply(int length) {
            used.set(true);
            return new byte[length];
        }
    }

    private FakeStatelessNode newNode() throws IOException {
        var home = PathUtils.get(createTempDir().toString());
        return new FakeStatelessNode(
            settings -> TestEnvironment.newEnvironment(withHome(settings, home)),
            settings -> new NodeEnvironment(withHome(settings, home), TestEnvironment.newEnvironment(withHome(settings, home))),
            xContentRegistry(),
            PRIMARY_TERM
        ) {
            @Override
            protected Settings nodeSettings() {
                // the cached delegate can only hand out memory segments when the shared cache is memory mapped, and only for ranges
                // that fall within a single region
                return Settings.builder()
                    .put(super.nodeSettings())
                    .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), true)
                    .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), pageAligned(ByteSizeValue.ofKb(256)))
                    .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(2))
                    .build();
            }
        };
    }

    private static Settings withHome(Settings settings, Path home) {
        return Settings.builder().put(settings).put(Environment.PATH_HOME_SETTING.getKey(), home.toAbsolutePath()).build();
    }

    private byte[] writeFile(FakeStatelessNode node, String fileName) throws IOException {
        var bytes = randomByteArrayOfLength(FILE_LENGTH);
        try (IndexOutput output = node.indexingDirectory.createOutput(fileName, IOContext.DEFAULT)) {
            output.writeBytes(bytes, bytes.length);
        }
        return bytes;
    }

    private static Path localIndexPath(FakeStatelessNode node) {
        return node.environment.dataDirs()[0].resolve(node.shardId.getIndex().getUUID()).resolve("0").resolve("index");
    }

    private static IndexDirectory.ReopeningIndexInput openReopeningInput(FakeStatelessNode node, String fileName) throws IOException {
        var input = node.indexingDirectory.openInput(fileName, IOContext.DEFAULT);
        assertThat(input, instanceOf(IndexDirectory.ReopeningIndexInput.class));
        return (IndexDirectory.ReopeningIndexInput) input;
    }

    private static void upload(FakeStatelessNode node, String fileName, byte[] bytes) throws IOException {
        node.indexingDirectory.getBlobStoreCacheDirectory()
            .getBlobContainer(PRIMARY_TERM)
            .writeBlob(
                OperationPurpose.INDICES,
                "stateless_commit_" + GENERATION,
                BytesReference.fromByteBuffer(ByteBuffer.wrap(bytes)),
                true
            );
        node.indexingDirectory.updateCommit(
            GENERATION,
            bytes.length,
            Set.of(fileName),
            Map.of(fileName, createBlobFileRanges(PRIMARY_TERM, GENERATION, 0L, bytes.length))
        );
    }

    private static void readFully(IndexInput input, byte[] expected) throws IOException {
        input.seek(0L);
        var actual = new byte[expected.length];
        input.readBytes(actual, 0, actual.length);
        assertArrayEquals(expected, actual);
    }

    private static void assertSegmentSlice(DirectAccessInput input, byte[] expected, long offset, int length) throws IOException {
        var invoked = new AtomicBoolean();
        var available = input.withMemorySegmentSlice(offset, length, segment -> {
            invoked.set(true);
            assertArrayEquals(Arrays.copyOfRange(expected, (int) offset, (int) offset + length), toByteArray(segment.asSlice(0, length)));
        });
        assertThat("withMemorySegmentSlice(" + offset + ", " + length + ") should be available", available, equalTo(true));
        assertThat(invoked.get(), equalTo(true));
    }

    private static void assertNoSegmentSlice(DirectAccessInput input, int length) throws IOException {
        var invoked = new AtomicBoolean();
        var available = input.withMemorySegmentSlice((long) 0, length, segment -> invoked.set(true));
        assertThat(available, equalTo(false));
        assertThat("action must not be invoked when no segment is available", invoked.get(), equalTo(false));
    }

    private static void assertSliceAddresses(DirectAccessInput input, byte[] expected, long[] offsets) throws IOException {
        var invoked = new AtomicBoolean();
        var available = input.withSliceAddresses(offsets, RANGE_LENGTH, offsets.length, addressesScratch(offsets.length), addresses -> {
            invoked.set(true);
            assertRangeAddresses(addresses, expected, offsets);
        });
        assertThat("withSliceAddresses should be available", available, equalTo(true));
        assertThat(invoked.get(), equalTo(true));
    }

    private static void assertNoSliceAddresses(DirectAccessInput input) throws IOException {
        var invoked = new AtomicBoolean();
        var available = input.withSliceAddresses(
            RANGE_OFFSETS,
            RANGE_LENGTH,
            RANGE_OFFSETS.length,
            addressesScratch(ReopeningIndexInputDirectAccessTests.RANGE_OFFSETS.length),
            addresses -> invoked.set(true)
        );
        assertThat(available, equalTo(false));
        assertThat("action must not be invoked when no addresses are available", invoked.get(), equalTo(false));
    }

    private static void assertSliceAddressesThroughUtils(IndexInput input, byte[] expected, boolean expectAvailable) throws IOException {
        var invoked = new AtomicBoolean();
        var available = IndexInputUtils.withSliceAddresses(
            input,
            RANGE_OFFSETS,
            RANGE_LENGTH,
            RANGE_OFFSETS.length,
            ReopeningIndexInputDirectAccessTests::addressesScratch,
            addresses -> {
                invoked.set(true);
                assertRangeAddresses(addresses, expected, RANGE_OFFSETS);
            }
        );
        assertThat(available, equalTo(expectAvailable));
        assertThat(invoked.get(), equalTo(expectAvailable));
    }

    private static void assertRangeAddresses(MemorySegment addresses, byte[] expected, long[] offsets) {
        for (int i = 0; i < offsets.length; i++) {
            var address = addresses.getAtIndex(ValueLayout.ADDRESS, i);
            assertThat("address " + i + " should not be null", address, not(equalTo(MemorySegment.NULL)));
            assertArrayEquals(
                "range at offset " + offsets[i],
                Arrays.copyOfRange(expected, (int) offsets[i], (int) offsets[i] + RANGE_LENGTH),
                toByteArray(address.reinterpret(RANGE_LENGTH))
            );
        }
    }

    private static byte[] toByteArray(MemorySegment segment) {
        var bytes = new byte[Math.toIntExact(segment.byteSize())];
        MemorySegment.ofArray(bytes).copyFrom(segment);
        return bytes;
    }

    private static MemorySegment addressesScratch(int count) {
        return Arena.ofAuto().allocate((long) count * ValueLayout.ADDRESS.byteSize(), ValueLayout.ADDRESS.byteAlignment());
    }

    private static MemorySegment misalignedScratch(int count) {
        return Arena.ofAuto().allocate((long) count * ValueLayout.ADDRESS.byteSize() + 1, 1).asSlice(1);
    }
}
