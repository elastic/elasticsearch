/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.lucene.store.ESIndexInputTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

import static org.elasticsearch.xpack.searchablesnapshots.cache.CacheFile.RANGE_SIZE;
import static org.hamcrest.Matchers.equalTo;

public class CacheBufferedIndexInputTests extends ESIndexInputTestCase {

    public void testRandomReads() throws IOException {
        final Settings cacheSettings = randomCacheSettings();
        final long cacheSize = CacheService.SNAPSHOT_CACHE_SIZE_SETTING.get(cacheSettings).getBytes();

        try (CacheService cacheService = new CacheService(cacheSettings)) {
            cacheService.start();

            for (int i = 0; i < 5; i++) {
                final String fileName = randomAlphaOfLength(10);
                final byte[] input = randomUnicodeOfLength(randomIntBetween(1, 100_000)).getBytes(StandardCharsets.UTF_8);

                Directory directory = new SingleFileDirectory(fileName, input);
                if (input.length <= cacheSize) {
                    directory = new CountingDirectory(directory);
                }

                try (CacheDirectory cacheDirectory = new CacheDirectory(directory, cacheService, createTempDir())) {
                    try (IndexInput indexInput = cacheDirectory.openInput(fileName, newIOContext(random()))) {
                        assertEquals(input.length, indexInput.length());
                        assertEquals(0, indexInput.getFilePointer());
                        byte[] output = randomReadAndSlice(indexInput, input.length);
                        assertArrayEquals(input, output);
                    }
                }

                if (directory instanceof CountingDirectory) {
                    long numberOfRanges = numberOfRanges(input.length);
                    assertThat("Expected " + numberOfRanges + " ranges fetched from the source",
                        ((CountingDirectory) directory).totalOpens.sum(), equalTo(numberOfRanges));
                    assertThat("All bytes should have been read from source",
                        ((CountingDirectory) directory).totalBytes.sum(), equalTo((long) input.length));
                }

                directory.close();
            }
        }
    }

    private static Settings randomCacheSettings() {
        final Settings.Builder cacheSettings = Settings.builder();
        if (randomBoolean()) {
            cacheSettings.put(CacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(),
                new ByteSizeValue(randomIntBetween(1, 100), randomFrom(ByteSizeUnit.BYTES, ByteSizeUnit.KB, ByteSizeUnit.MB)));
        }
        return cacheSettings.build();
    }

    private static long numberOfRanges(int fileSize) {
        long numberOfRanges = fileSize / RANGE_SIZE;
        if (fileSize % RANGE_SIZE > 0) {
            numberOfRanges++;
        }
        if (numberOfRanges == 0) {
            numberOfRanges++;
        }
        return numberOfRanges;
    }

    /**
     * FilterDirectory that provides a single IndexInput with a given name and content.
     */
    private static class SingleFileDirectory  extends FilterDirectory {

        private final String fileName;
        private final byte[] fileContent;

        SingleFileDirectory(final String fileName, final byte[] fileContent) {
            super(null);
            this.fileName = Objects.requireNonNull(fileName);
            this.fileContent = Objects.requireNonNull(fileContent);
        }

        @Override
        public String[] listAll() {
            return new String[]{fileName};
        }

        @Override
        public long fileLength(String name) throws IOException {
            if (name.equals(fileName)) {
                return fileContent.length;
            }
            throw new FileNotFoundException(name);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            if (name.equals(fileName)) {
                return new ByteArrayIndexInput(fileName, fileContent);
            }
            throw new FileNotFoundException(name);
        }

        @Override
        public void close() {
        }
    }

    /**
     * FilterDirectory that counts the number of IndexInput it opens, as well as the
     * total number of bytes read from them.
     */
    private static class CountingDirectory extends FilterDirectory {

        private final LongAdder totalBytes = new LongAdder();
        private final LongAdder totalOpens = new LongAdder();

        CountingDirectory(Directory in) {
            super(in);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            return new CountingIndexInput(this, super.openInput(name, context));
        }
    }

    /**
     * IndexInput that counts the number of bytes read from it, as well as the positions
     * where read operations start and finish.
     */
    private static class CountingIndexInput extends IndexInput {

        private final CountingDirectory dir;
        private final IndexInput in;

        private long bytesRead = 0L;
        private long start = Long.MAX_VALUE;
        private long end = Long.MIN_VALUE;

        CountingIndexInput(CountingDirectory directory, IndexInput input) {
            super("CountingIndexInput(" + input + ")");
            this.dir = Objects.requireNonNull(directory);
            this.in = Objects.requireNonNull(input);
            dir.totalOpens.increment();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            if (getFilePointer() < start) {
                start = getFilePointer();
            }

            in.readBytes(b, offset, len);
            bytesRead += len;

            if (getFilePointer() > end) {
                end = getFilePointer();
            }
        }

        @Override
        public byte readByte() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getFilePointer() {
            return in.getFilePointer();
        }

        @Override
        public void seek(long pos) throws IOException {
            in.seek(pos);
        }

        @Override
        public long length() {
            return in.length();
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return new CountingIndexInput(dir, in.slice(sliceDescription, offset, length));
        }

        @Override
        public IndexInput clone() {
            return new CountingIndexInput(dir, in.clone());
        }

        @Override
        public void close() throws IOException {
            in.close();
            if (start % RANGE_SIZE != 0) {
                throw new AssertionError("Read operation should start at the beginning of a range");
            }
            if (end % RANGE_SIZE != 0) {
                if (end != in.length()) {
                    throw new AssertionError("Read operation should finish at the end of a range or the end of the file");
                }
            }
            if (in.length() <= RANGE_SIZE) {
                if (bytesRead != in.length()) {
                    throw new AssertionError("All [" + in.length() + "] bytes should have been read, no more no less but got:" + bytesRead);
                }
            } else {
                if (bytesRead != RANGE_SIZE) {
                    if (end != in.length()) {
                        throw new AssertionError("Expecting [" + RANGE_SIZE + "] bytes to be read but got:" + bytesRead);

                    }
                    final long remaining = in.length() % RANGE_SIZE;
                    if (bytesRead != remaining) {
                        throw new AssertionError("Expecting [" + remaining + "] bytes to be read but got:" + bytesRead);
                    }
                }
            }
            dir.totalBytes.add(bytesRead);
        }
    }

}
