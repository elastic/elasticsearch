/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.store.SearchableSnapshotDirectory.LiveCounter;
import org.elasticsearch.index.store.stats.IndexInputStats;
import org.elasticsearch.index.store.stats.IndexInputStats.Counter;
import org.elasticsearch.test.ESTestCase;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.store.SearchableSnapshotDirectoryTests.randomFile;
import static org.elasticsearch.index.store.SearchableSnapshotDirectoryTests.testDirectories;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SearchableSnapshotDirectoryStatsTests extends ESTestCase {

    public void testLength() throws Exception {
        testDirectories((directory, snapshotDirectory) -> {
            final String fileName = randomFile(snapshotDirectory);
            try (IndexInput file = snapshotDirectory.openInput(fileName, newIOContext(random()))) {
                assertThat("File [" + fileName + "] length reported in stats does not match expected length",
                    snapshotDirectory.getStatsOrNull(fileName).getLength(), equalTo(directory.fileLength(fileName)));
            }
        });
    }

    public void testOpenCount() throws Exception {
        testDirectories((directory, snapshotDirectory) -> {
            assertThat(snapshotDirectory.getStats(), notNullValue());
            assertThat(snapshotDirectory.getStats().isEmpty(), is(true));

            final String fileName = randomFile(snapshotDirectory);
            assertThat(snapshotDirectory.getStatsOrNull(fileName), nullValue());

            for (long i = 1L; i <= randomIntBetween(1, 10); i++) {
                try (IndexInput input = snapshotDirectory.openInput(fileName, newIOContext(random()))) {
                    IndexInputStats stats = snapshotDirectory.getStatsOrNull(fileName);
                    assertThat(stats, notNullValue());
                    assertThat(stats.getOpenCount(), equalTo(i));

                    if (randomBoolean()) {
                        try (IndexInput clone = input.clone()) {
                            stats = snapshotDirectory.getStatsOrNull(fileName);
                            assertThat("clones are not counted in opening count", stats.getOpenCount(), equalTo(i));
                        }
                    }
                    if (randomBoolean()) {
                        try (IndexInput slice = input.slice("slice", 0, input.length())) {
                            stats = snapshotDirectory.getStatsOrNull(fileName);
                            assertThat("slices are not counted in opening count", stats.getOpenCount(), equalTo(i));
                        }
                    }
                }
            }
        });
    }

    public void testCloseCount() throws Exception {
        testDirectories((directory, snapshotDirectory) -> {
            assertThat(snapshotDirectory.getStats(), notNullValue());
            assertThat(snapshotDirectory.getStats().isEmpty(), is(true));

            final String fileName = randomFile(snapshotDirectory);
            assertThat(snapshotDirectory.getStatsOrNull(fileName), nullValue());

            for (long i = 1L; i <= randomIntBetween(1, 10); i++) {
                try (IndexInput input = snapshotDirectory.openInput(fileName, newIOContext(random()))) {
                    if (randomBoolean()) {
                        // clones are not counted in opening count
                        IndexInput clone = input.clone();
                        clone.close();
                    }
                    if (randomBoolean()) {
                        // slices are not counted in opening count
                        IndexInput slice = input.slice("slice", 0, input.length());
                        slice.close();
                    }
                }

                final IndexInputStats stats = snapshotDirectory.getStatsOrNull(fileName);
                assertThat(stats, notNullValue());
                assertThat(stats.getCloseCount(), equalTo(i));
            }
        });
    }

    public void testTotalSeekCount() throws Exception {
        testDirectories((directory, snapshotDirectory) -> {
            final String fileName = randomFile(snapshotDirectory);

            try (IndexInput input = snapshotDirectory.openInput(fileName, newIOContext(random()))) {
                for (long i = 1L; i <= randomIntBetween(1, 10); i++) {
                    switch (randomInt(2)) {
                        case 0:
                            input.seek(randomLongBetween(0L, input.length()));
                            break;
                        case 1:
                            IndexInput clone = input.clone();
                            clone.seek(randomLongBetween(0L, clone.length()));
                            break;
                        case 2:
                            IndexInput slice = input.slice("slice", 0, input.length());
                            slice.seek(randomLongBetween(0L, slice.length()));
                            break;
                        default:
                            fail();
                    }

                    final IndexInputStats stats = snapshotDirectory.getStatsOrNull(fileName);
                    assertThat(stats.getTotalSeeks().getCount(), equalTo(i));
                }
            }
        });
    }

    public void testForwardSeekCount() throws Exception {
        final List<Closeable> closeables = new ArrayList<>();
        testDirectories((directory, snapshotDirectory) -> {
            final String fileName = randomFile(snapshotDirectory);

            IndexInput input = snapshotDirectory.openInput(fileName, newIOContext(random()));
            closeables.add(input);

            if (randomBoolean()) {
                input = input.clone();
            }
            if (randomBoolean()) {
                input = input.slice("slice", 0, input.length());
                closeables.add(input);
            }

            IndexInputStats stats = snapshotDirectory.getStatsOrNull(fileName);

            assertThat(stats.getForwardSmallSeeks().getCount(), equalTo(0L));
            assertThat(stats.getForwardSmallSeeks().getMin(), equalTo(0L));
            assertThat(stats.getForwardSmallSeeks().getMax(), equalTo(0L));

            assertThat(stats.getForwardLargeSeeks().getCount(), equalTo(0L));
            assertThat(stats.getForwardLargeSeeks().getMin(), equalTo(0L));
            assertThat(stats.getForwardLargeSeeks().getMax(), equalTo(0L));

            long maxSmall = Long.MIN_VALUE;
            long minSmall = Long.MAX_VALUE;
            long countSmall = 0;

            long minLarge = Long.MAX_VALUE;
            long maxLarge = Long.MIN_VALUE;
            long countLarge = 0;

            for (long i = 1; i < 10; i++) {
                final long seekTo = randomLongBetween(input.getFilePointer(), input.length());

                long delta = seekTo - input.getFilePointer();
                if (delta == 0L) {
                    break;
                }

                input.seek(seekTo);
                stats = snapshotDirectory.getStatsOrNull(fileName);
                if (delta < (input.length() * 0.25)) {
                    minSmall = Math.min(minSmall, delta);
                    maxSmall = Math.max(maxSmall, delta);
                    assertThat(stats.getForwardSmallSeeks().getCount(), equalTo(++countSmall));
                    assertThat(stats.getForwardSmallSeeks().getMin(), equalTo(minSmall));
                    assertThat(stats.getForwardSmallSeeks().getMax(), equalTo(maxSmall));

                } else {
                    minLarge = Math.min(minLarge, delta);
                    maxLarge = Math.max(maxLarge, delta);
                    assertThat(stats.getForwardLargeSeeks().getCount(), equalTo(++countLarge));
                    assertThat(stats.getForwardLargeSeeks().getMin(), equalTo(minLarge));
                    assertThat(stats.getForwardLargeSeeks().getMax(), equalTo(maxLarge));
                }
            }

            IOUtils.close(closeables);
        });
    }

    public void testBackwardSeekCount() throws Exception {
        final List<Closeable> closeables = new ArrayList<>();
        testDirectories((directory, snapshotDirectory) -> {
            final String fileName = randomFile(snapshotDirectory);

            IndexInput input = snapshotDirectory.openInput(fileName, newIOContext(random()));
            closeables.add(input);

            if (randomBoolean()) {
                input = input.clone();
            }
            if (randomBoolean()) {
                input = input.slice("slice", 0, input.length());
                closeables.add(input);
            }

            // seek to the end
            input.seek(input.length());

            IndexInputStats stats = snapshotDirectory.getStats().get(fileName);

            assertThat(stats.getBackwardSmallSeeks().getCount(), equalTo(0L));
            assertThat(stats.getBackwardSmallSeeks().getMin(), equalTo(0L));
            assertThat(stats.getBackwardSmallSeeks().getMax(), equalTo(0L));

            assertThat(stats.getBackwardLargeSeeks().getCount(), equalTo(0L));
            assertThat(stats.getBackwardLargeSeeks().getMin(), equalTo(0L));
            assertThat(stats.getBackwardLargeSeeks().getMax(), equalTo(0L));

            long maxSmall = Long.MIN_VALUE;
            long minSmall = Long.MAX_VALUE;
            long countSmall = 0;

            long minLarge = Long.MAX_VALUE;
            long maxLarge = Long.MIN_VALUE;
            long countLarge = 0;

            for (long i = 1; i < 10; i++) {
                final long seekTo = randomLongBetween(0L, input.getFilePointer());

                long delta = seekTo - input.getFilePointer();
                if (delta == 0L) {
                    break;
                }

                input.seek(seekTo);
                stats = snapshotDirectory.getStatsOrNull(fileName);
                if (delta > -(input.length() * 0.25)) {
                    minSmall = Math.min(minSmall, delta);
                    maxSmall = Math.max(maxSmall, delta);
                    assertThat(stats.getBackwardSmallSeeks().getCount(), equalTo(++countSmall));
                    assertThat(stats.getBackwardSmallSeeks().getMin(), equalTo(minSmall));
                    assertThat(stats.getBackwardSmallSeeks().getMax(), equalTo(maxSmall));

                } else {
                    minLarge = Math.min(minLarge, delta);
                    maxLarge = Math.max(maxLarge, delta);
                    assertThat(stats.getBackwardLargeSeeks().getCount(), equalTo(++countLarge));
                    assertThat(stats.getBackwardLargeSeeks().getMin(), equalTo(minLarge));
                    assertThat(stats.getBackwardLargeSeeks().getMax(), equalTo(maxLarge));
                }
            }

            IOUtils.close(closeables);
        });
    }

    public void testRandomSeeks() throws Exception {
        final List<Closeable> closeables = new ArrayList<>();
        testDirectories((directory, snapshotDirectory) -> {
            final String fileName = randomFile(snapshotDirectory);

            IndexInput input = snapshotDirectory.openInput(fileName, newIOContext(random()));
            closeables.add(input);

            if (randomBoolean()) {
                input = input.clone();
            }
            if (randomBoolean()) {
                input = input.slice("slice", 0, input.length());
                closeables.add(input);
            }

            IndexInputStats stats = snapshotDirectory.getStatsOrNull(fileName);
            assertThat(stats.getTotalSeeks().getCount(), equalTo(0L));

            final LiveCounter forwardSmallSeeks = new LiveCounter();
            final LiveCounter forwardLargeSeeks = new LiveCounter();

            final LiveCounter backwardSmallSeeks = new LiveCounter();
            final LiveCounter backwardLargeSeeks = new LiveCounter();

            final long iterations = randomIntBetween(10, 25);
            for (long i = 1; i <= iterations; i++) {

                final long seekTo = randomLongBetween(0, input.length());
                final long delta = seekTo - input.getFilePointer();

                input.seek(seekTo);

                if (delta >= 0) {
                    LiveCounter forwardCounter = (delta < (input.length() * 0.25d)) ? forwardSmallSeeks : forwardLargeSeeks;
                    forwardCounter.add(delta);
                } else {
                    LiveCounter backwardCounter = (delta > -(input.length() * 0.25d)) ? backwardSmallSeeks : backwardLargeSeeks;
                    backwardCounter.add(delta);
                }
            }

            stats = snapshotDirectory.getStatsOrNull(fileName);

            assertThat(stats.getForwardSmallSeeks(), equalTo(forwardSmallSeeks.toCounter()));
            assertThat(stats.getForwardLargeSeeks(), equalTo(forwardLargeSeeks.toCounter()));
            assertThat(stats.getBackwardSmallSeeks(), equalTo(backwardSmallSeeks.toCounter()));
            assertThat(stats.getBackwardLargeSeeks(), equalTo(backwardLargeSeeks.toCounter()));

            Counter totalSeeks = stats.getTotalSeeks();

            assertThat(totalSeeks.getCount(), equalTo(iterations));
            assertThat(totalSeeks.getCount(),
                equalTo(forwardSmallSeeks.count() + forwardLargeSeeks.count() + backwardSmallSeeks.count() + backwardLargeSeeks.count()));
            assertThat(totalSeeks.getTotal(),
                equalTo(forwardSmallSeeks.total() + forwardLargeSeeks.total() + backwardSmallSeeks.total() + backwardLargeSeeks.total()));

            IOUtils.close(closeables);
        });
    }

    public void testReads() throws Exception {
        final List<Closeable> closeables = new ArrayList<>();
        testDirectories((directory, snapshotDirectory) -> {
            final String fileName = randomFile(snapshotDirectory);

            IndexInput input = snapshotDirectory.openInput(fileName, newIOContext(random()));
            closeables.add(input);

            if (randomBoolean()) {
                input = input.clone();
            }
            if (randomBoolean()) {
                input = input.slice("slice", 0, input.length());
                closeables.add(input);
            }

            long contiguousReadsMinValue = Long.MAX_VALUE;
            long contiguousReadsMaxValue = Long.MIN_VALUE;
            long contiguousReadsCount = 0L;
            long contiguousReadsTotal = 0L;

            Counter contiguousReads = snapshotDirectory.getStatsOrNull(fileName).getContiguousReads();
            assertThat(contiguousReads.getCount(), equalTo(0L));
            assertThat(contiguousReads.getTotal(), equalTo(0L));
            assertThat(contiguousReads.getMin(), equalTo(0L));
            assertThat(contiguousReads.getMax(), equalTo(0L));

            long nonContiguousReadsMinValue = Long.MAX_VALUE;
            long nonContiguousReadsMaxValue = Long.MIN_VALUE;
            long nonContiguousReadsCount = 0L;
            long nonContiguousReadsTotal = 0L;

            Counter nonContiguousReads = snapshotDirectory.getStatsOrNull(fileName).getNonContiguousReads();
            assertThat(nonContiguousReads.getCount(), equalTo(0L));
            assertThat(nonContiguousReads.getTotal(), equalTo(0L));
            assertThat(nonContiguousReads.getMin(), equalTo(0L));
            assertThat(nonContiguousReads.getMax(), equalTo(0L));

            long pos = 0L;
            while (pos < input.length()) {
                final boolean nonContiguous = randomBoolean() & (pos < input.length() - 1L);
                if (nonContiguous) {
                    pos = randomLongBetween(pos + 1L, input.length() - 1L);
                    input.seek(pos);
                    assertThat(input.getFilePointer(), equalTo(pos));
                }

                int len;
                if (randomBoolean()) {
                    len = 1;
                    input.readByte();
                } else {
                    len = randomIntBetween(1, Math.toIntExact(input.length() - pos));
                    input.readBytes(new byte[len], 0, len);
                }

                if (nonContiguous) {
                    nonContiguousReadsMinValue = Math.min(len, nonContiguousReadsMinValue);
                    nonContiguousReadsMaxValue = Math.max(len, nonContiguousReadsMaxValue);
                    nonContiguousReadsCount += 1L;
                    nonContiguousReadsTotal += len;

                } else {
                    contiguousReadsMinValue = Math.min(len, contiguousReadsMinValue);
                    contiguousReadsMaxValue = Math.max(len, contiguousReadsMaxValue);
                    contiguousReadsCount += 1L;
                    contiguousReadsTotal += len;
                }
                pos += len;
            }

            contiguousReads = snapshotDirectory.getStatsOrNull(fileName).getContiguousReads();
            assertThat(contiguousReads.getCount(), equalTo(contiguousReadsCount));
            assertThat(contiguousReads.getTotal(), equalTo(contiguousReadsTotal));
            if (contiguousReads.getCount() > 0) {
                assertThat(contiguousReads.getMin(), equalTo(contiguousReadsMinValue));
                assertThat(contiguousReads.getMax(), equalTo(contiguousReadsMaxValue));
            }

            nonContiguousReads = snapshotDirectory.getStatsOrNull(fileName).getNonContiguousReads();
            assertThat(nonContiguousReads.getCount(), equalTo(nonContiguousReadsCount));
            assertThat(nonContiguousReads.getTotal(), equalTo(nonContiguousReadsTotal));
            if (nonContiguousReads.getCount() > 0) {
                assertThat(nonContiguousReads.getMin(), equalTo(nonContiguousReadsMinValue));
                assertThat(nonContiguousReads.getMax(), equalTo(nonContiguousReadsMaxValue));
            }

            Counter totalReads = snapshotDirectory.getStatsOrNull(fileName).getTotalReads();
            assertThat(totalReads.getCount(), equalTo(contiguousReadsCount + nonContiguousReadsCount));
            assertThat(totalReads.getTotal(), equalTo(contiguousReadsTotal + nonContiguousReadsTotal));

            IOUtils.close(closeables);
        });
    }

    public void testRandomReads() throws Exception {
        testDirectories((directory, snapshotDirectory) -> {
            for (String fileName : snapshotDirectory.listAll()) {
                if (fileName.startsWith("extra") || fileName.equals("write.lock")) {
                    continue;
                }
                try (IndexInput input = snapshotDirectory.openInput(fileName, newIOContext(random()))) {

                    Counter totalReads = snapshotDirectory.getStatsOrNull(fileName).getTotalReads();
                    assertThat(totalReads.getTotal(), equalTo(0L));
                    assertThat(totalReads.getCount(), equalTo(0L));

                    Tuple<Long, Long> results = randomReads(input, (int) input.length());

                    totalReads = snapshotDirectory.getStatsOrNull(fileName).getTotalReads();
                    assertThat(totalReads.getTotal(), equalTo(input.length()));
                    assertThat(totalReads.getTotal(), equalTo(results.v1()));
                    assertThat(totalReads.getCount(), equalTo(results.v2()));
                }
            }
        });
    }

    private Tuple<Long, Long> randomReads(final IndexInput indexInput, final int length) throws IOException {
        long bytesReadTotal = 0L;
        long bytesReadCount = 0L;

        int position = (int) indexInput.getFilePointer();
        while (position < length) {
            switch (randomInt(3)) {
                case 0:
                    // Read one byte
                    indexInput.readByte();
                    bytesReadTotal += 1L;
                    bytesReadCount++;
                    position += 1;
                    break;
                case 1:
                    // Read several bytes
                    int len = randomIntBetween(1, length - position);
                    indexInput.readBytes(new byte[len], 0, len);
                    bytesReadTotal += len;
                    bytesReadCount++;
                    position += len;
                    break;
                case 2:
                    // Read using slice
                    len = randomIntBetween(1, length - position);
                    IndexInput slice = indexInput.slice("slice (" + position + ", " + len + ") of " + indexInput.toString(), position, len);
                    Tuple<Long, Long> results = randomReads(slice, len);
                    slice.close();
                    bytesReadTotal += results.v1();
                    bytesReadCount += results.v2();
                    assertEquals(position, indexInput.getFilePointer());
                    position += len;
                    indexInput.seek(position);
                    break;
                case 3:
                    // Read using clone
                    len = randomIntBetween(1, length - position);
                    IndexInput clone = indexInput.clone();
                    results = randomReads(clone, position + len);
                    assertEquals(position + len, clone.getFilePointer());
                    bytesReadTotal += results.v1();
                    bytesReadCount += results.v2();
                    assertEquals(position, indexInput.getFilePointer());
                    position += len;
                    indexInput.seek(position);
                    break;
                default:
                    fail();
            }
            assertEquals(position, indexInput.getFilePointer());
        }
        return Tuple.tuple(bytesReadTotal, bytesReadCount);
    }
}
