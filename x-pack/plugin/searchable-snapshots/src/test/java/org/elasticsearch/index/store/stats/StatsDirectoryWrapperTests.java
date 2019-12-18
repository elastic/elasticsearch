/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store.stats;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class StatsDirectoryWrapperTests extends ESTestCase {

    public void testListAll() throws Exception {
        final Directory directory = createDirectory();
        assertArrayEquals(directory.listAll(), new StatsDirectoryWrapper(directory).listAll());
        directory.close();
    }

    public void testLength() throws Exception {
        try (StatsDirectoryWrapper directory = new StatsDirectoryWrapper(createDirectory())) {
            final String fileName = randomFile(directory);
            try (IndexInput input = directory.openInput(fileName, newIOContext(random()))) {
                assertThat(directory.getStatsOrNull(fileName).getLength(), equalTo(input.length()));
            }
        }
    }

    public void testOpenCount() throws Exception {
        try (StatsDirectoryWrapper directory = new StatsDirectoryWrapper(createDirectory())) {
            assertThat(directory.getStats(), notNullValue());
            assertThat(directory.getStats().isEmpty(), is(true));

            final String fileName = randomFile(directory);
            assertThat(directory.getStatsOrNull(fileName), nullValue());

            for (long i = 1L; i <= randomIntBetween(1, 10); i++) {
                try (IndexInput input = directory.openInput(fileName, newIOContext(random()))) {
                    IndexInputStats stats = directory.getStatsOrNull(fileName);
                    assertThat(stats, notNullValue());
                    assertThat(stats.getOpenCount(), equalTo(i));

                    if (randomBoolean()) {
                        try (IndexInput clone = input.clone()) {
                            stats = directory.getStatsOrNull(fileName);
                            assertThat("clones are not counted in opening count", stats.getOpenCount(), equalTo(i));
                        }
                    }
                    if (randomBoolean()) {
                        try (IndexInput slice = input.slice("slice", 0, input.length())) {
                            stats = directory.getStatsOrNull(fileName);
                            assertThat("slices are not counted in opening count", stats.getOpenCount(), equalTo(i));
                        }
                    }
                }
            }
        }
    }

    public void testCloseCount() throws Exception {
        try (StatsDirectoryWrapper directory = new StatsDirectoryWrapper(createDirectory())) {
            assertThat(directory.getStats(), notNullValue());
            assertThat(directory.getStats().isEmpty(), is(true));

            final String fileName = randomFile(directory);
            assertThat(directory.getStatsOrNull(fileName), nullValue());

            for (long i = 1L; i <= randomIntBetween(1, 10); i++) {
                try (IndexInput input = directory.openInput(fileName, newIOContext(random()))) {
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

                final IndexInputStats stats = directory.getStatsOrNull(fileName);
                assertThat(stats, notNullValue());
                assertThat(stats.getCloseCount(), equalTo(i));
            }
        }
    }

    public void testTotalSeekCount() throws Exception {
        try (StatsDirectoryWrapper directory = new StatsDirectoryWrapper(createDirectory())) {
            final String fileName = randomFile(directory);

            try (IndexInput input = directory.openInput(fileName, newIOContext(random()))) {
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

                    final IndexInputStats stats = directory.getStatsOrNull(fileName);
                    assertThat(stats.getTotalSeeks().getCount(), equalTo(i));
                }
            }
        }
    }

    public void testForwardSeekCount() throws Exception {
        final List<Closeable> closeables = new ArrayList<>();
        try (StatsDirectoryWrapper directory = new StatsDirectoryWrapper(createDirectory())) {
            final String fileName = randomFile(directory);

            IndexInput input = directory.openInput(fileName, newIOContext(random()));
            closeables.add(input);

            if (randomBoolean()) {
                input = input.clone();
            }
            if (randomBoolean()) {
                input = input.slice("slice", 0, input.length());
                closeables.add(input);
            }

            IndexInputStats stats = directory.getStatsOrNull(fileName);
            assertThat(stats.getForwardSeeks().getCount(), equalTo(0L));
            assertThat(stats.getForwardSeeks().getMin(), equalTo(0L));
            assertThat(stats.getForwardSeeks().getMax(), equalTo(0L));

            long max = Long.MIN_VALUE;
            long min = Long.MAX_VALUE;

            for (long i = 1; i < 10; i++) {
                final long seekTo = randomLongBetween(input.getFilePointer(), input.length());

                long delta = seekTo - input.getFilePointer();
                if (delta == 0L) {
                    break;
                }

                min = Math.min(min, delta);
                max = Math.max(max, delta);

                input.seek(seekTo);

                stats = directory.getStatsOrNull(fileName);
                assertThat(stats.getForwardSeeks().getCount(), equalTo(i));
                assertThat(stats.getForwardSeeks().getMin(), equalTo(min));
                assertThat(stats.getForwardSeeks().getMax(), equalTo(max));
            }

            IOUtils.close(closeables);
        }
    }

    public void testBackwardSeekCount() throws Exception {
        final List<Closeable> closeables = new ArrayList<>();
        try (StatsDirectoryWrapper directory = new StatsDirectoryWrapper(createDirectory())) {
            final String fileName = randomFile(directory);

            IndexInput input = directory.openInput(fileName, newIOContext(random()));
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

            IndexInputStats stats = directory.getStats().get(fileName);
            assertThat(stats.getBackwardSeeks().getCount(), equalTo(0L));
            assertThat(stats.getBackwardSeeks().getMin(), equalTo(0L));
            assertThat(stats.getBackwardSeeks().getMax(), equalTo(0L));

            long max = Long.MIN_VALUE;
            long min = Long.MAX_VALUE;

            for (long i = 1; i < 10; i++) {
                final long seekTo = randomLongBetween(0L, input.getFilePointer());

                long delta = input.getFilePointer() - seekTo;
                if (delta == 0L) {
                    break;
                }

                min = Math.min(min, delta);
                max = Math.max(max, delta);

                input.seek(seekTo);

                stats = directory.getStatsOrNull(fileName);
                assertThat(stats.getBackwardSeeks().getCount(), equalTo(i));
                assertThat(stats.getBackwardSeeks().getMin(), equalTo(min));
                assertThat(stats.getBackwardSeeks().getMax(), equalTo(max));
            }

            IOUtils.close(closeables);
        }
    }

    public void testRandomSeeks() throws Exception {
        final List<Closeable> closeables = new ArrayList<>();
        try (StatsDirectoryWrapper directory = new StatsDirectoryWrapper(createDirectory())) {
            final String fileName = randomFile(directory);

            IndexInput input = directory.openInput(fileName, newIOContext(random()));
            closeables.add(input);

            if (randomBoolean()) {
                input = input.clone();
            }
            if (randomBoolean()) {
                input = input.slice("slice", 0, input.length());
                closeables.add(input);
            }

            IndexInputStats stats = directory.getStatsOrNull(fileName);
            assertThat(stats.getTotalSeeks().getCount(), equalTo(0L));

            long forwardSeekCount = 0L;
            long minForwardSeekValue = Long.MAX_VALUE;
            long maxForwardSeekValue = Long.MIN_VALUE;

            long backwardSeekCount = 0L;
            long minBackwardSeekValue = Long.MAX_VALUE;
            long maxBackwardSeekValue = Long.MIN_VALUE;

            final long iterations = randomIntBetween(10, 25);
            for (long i = 1; i <= iterations; i++) {

                final long seekTo = randomLongBetween(0, input.length());
                final long delta = seekTo - input.getFilePointer();

                input.seek(seekTo);

                stats = directory.getStatsOrNull(fileName);
                if (delta >= 0) {
                    assertThat(stats.getForwardSeeks().getCount(), equalTo(forwardSeekCount + 1L));
                    forwardSeekCount = stats.getForwardSeeks().getCount();

                    assertThat(stats.getForwardSeeks().getMin(), equalTo(Math.min(minForwardSeekValue, delta)));
                    minForwardSeekValue = stats.getForwardSeeks().getMin();

                    assertThat(stats.getForwardSeeks().getMax(), equalTo(Math.max(maxForwardSeekValue, delta)));
                    maxForwardSeekValue = stats.getForwardSeeks().getMax();

                } else {
                    assertThat(stats.getBackwardSeeks().getCount(), equalTo(backwardSeekCount + 1L));
                    backwardSeekCount = stats.getBackwardSeeks().getCount();

                    assertThat(stats.getBackwardSeeks().getMin(), equalTo(Math.min(minBackwardSeekValue, Math.abs(delta))));
                    minBackwardSeekValue = stats.getBackwardSeeks().getMin();

                    assertThat(stats.getBackwardSeeks().getMax(), equalTo(Math.max(maxBackwardSeekValue, Math.abs(delta))));
                    maxBackwardSeekValue = stats.getBackwardSeeks().getMax();
                }
            }

            IndexInputStats.Counter totalSeeks = directory.getStatsOrNull(fileName).getTotalSeeks();
            assertThat(totalSeeks.getCount(), equalTo(iterations));
            assertThat(totalSeeks.getCount(), equalTo(forwardSeekCount + backwardSeekCount));
            assertThat(totalSeeks.getMin(), equalTo(Math.min(minForwardSeekValue, minBackwardSeekValue)));
            assertThat(totalSeeks.getMax(), equalTo(Math.max(maxForwardSeekValue, maxBackwardSeekValue)));

            IOUtils.close(closeables);
        }
    }

    public void testReads() throws Exception {
        final List<Closeable> closeables = new ArrayList<>();
        try (StatsDirectoryWrapper directory = new StatsDirectoryWrapper(createDirectory())) {
            final String fileName = randomFile(directory);

            IndexInput input = directory.openInput(fileName, newIOContext(random()));
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

            IndexInputStats.Counter contiguousReads = directory.getStatsOrNull(fileName).getContiguousReads();
            assertThat(contiguousReads.getCount(), equalTo(0L));
            assertThat(contiguousReads.getTotal(), equalTo(0L));
            assertThat(contiguousReads.getMin(), equalTo(0L));
            assertThat(contiguousReads.getMax(), equalTo(0L));

            long nonContiguousReadsMinValue = Long.MAX_VALUE;
            long nonContiguousReadsMaxValue = Long.MIN_VALUE;
            long nonContiguousReadsCount = 0L;
            long nonContiguousReadsTotal = 0L;

            IndexInputStats.Counter nonContiguousReads = directory.getStatsOrNull(fileName).getNonContiguousReads();
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

            contiguousReads = directory.getStatsOrNull(fileName).getContiguousReads();
            assertThat(contiguousReads.getCount(), equalTo(contiguousReadsCount));
            assertThat(contiguousReads.getTotal(), equalTo(contiguousReadsTotal));
            if (contiguousReads.getCount() > 0) {
                assertThat(contiguousReads.getMin(), equalTo(contiguousReadsMinValue));
                assertThat(contiguousReads.getMax(), equalTo(contiguousReadsMaxValue));
            }

            nonContiguousReads = directory.getStatsOrNull(fileName).getNonContiguousReads();
            assertThat(nonContiguousReads.getCount(), equalTo(nonContiguousReadsCount));
            assertThat(nonContiguousReads.getTotal(), equalTo(nonContiguousReadsTotal));
            if (nonContiguousReads.getCount() > 0) {
                assertThat(nonContiguousReads.getMin(), equalTo(nonContiguousReadsMinValue));
                assertThat(nonContiguousReads.getMax(), equalTo(nonContiguousReadsMaxValue));
            }

            IndexInputStats.Counter totalReads = directory.getStatsOrNull(fileName).getTotalReads();
            assertThat(totalReads.getCount(), equalTo(contiguousReadsCount + nonContiguousReadsCount));
            assertThat(totalReads.getTotal(), equalTo(contiguousReadsTotal + nonContiguousReadsTotal));

            IOUtils.close(closeables);
        }
    }

    public void testRandomReads() throws Exception {
        try (StatsDirectoryWrapper directory = new StatsDirectoryWrapper(createDirectory())) {
            for (String fileName : directory.listAll()) {
                try (IndexInput input = directory.openInput(fileName, newIOContext(random()))) {

                    IndexInputStats.Counter totalReads = directory.getStatsOrNull(fileName).getTotalReads();
                    assertThat(totalReads.getTotal(), equalTo(0L));
                    assertThat(totalReads.getCount(), equalTo(0L));

                    Tuple<Long, Long> results = randomReads(input, (int) input.length());

                    totalReads = directory.getStatsOrNull(fileName).getTotalReads();
                    assertThat(totalReads.getTotal(), allOf(equalTo(results.v1()), equalTo(input.length())));
                    assertThat(totalReads.getCount(), equalTo(results.v2()));
                }
            }
        }
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

    private static Directory createDirectory() throws IOException {
        final Directory directory = newDirectory(random());
        for (int i = 0; i < randomIntBetween(1, 25); i++) {
            try (IndexOutput indexOutput = directory.createOutput("file_" + i, newIOContext(random()))) {
                byte[] data = randomUnicodeOfLength(randomIntBetween(1, 1 << 10)).getBytes(StandardCharsets.UTF_8);
                indexOutput.writeBytes(data, data.length);
            }
        }
        return directory;
    }

    private static String randomFile(final Directory directory) throws Exception {
        while (true) {
            final String fileName = randomFrom(directory.listAll());
            if (fileName.startsWith("extra") || fileName.equals("write.lock")) {
                continue;
            }
            return fileName;
        }
    }
}
