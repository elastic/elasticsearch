/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.lucene;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.index.store.StoreMetrics;
import org.elasticsearch.index.store.StoreMetricsDirectory;
import org.elasticsearch.index.store.StoreMetricsIndexInput;
import org.elasticsearch.index.store.ThreadLocalDirectoryMetricHolder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Path;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * Tests that a search directory accounts the bytes read through it once it is given a holder.
 */
public class SearchDirectoryStoreMetricsTests extends ESTestCase {

    private static final String FILE_NAME = "_0.cfs";

    public void testAccountsTheBytesReadThroughTheCache() throws IOException {
        final Path dataPath = createTempDir();
        // written before the directory is opened, as a search node only ever reads files written elsewhere
        final byte[] bytes = randomByteArrayOfLength(8192);
        try (Directory writeDir = new NIOFSDirectory(dataPath); IndexOutput out = writeDir.createOutput(FILE_NAME, IOContext.DEFAULT)) {
            out.writeBytes(bytes, bytes.length);
        }

        try (Directory directory = StatelessDirectoryFactory.newSearchDirectory(dataPath, createTempDir())) {
            final var metrics = new ThreadLocalDirectoryMetricHolder<>(StoreMetrics::new);

            try (IndexInput input = directory.openInput(FILE_NAME, IOContext.DEFAULT)) {
                input.readBytes(new byte[bytes.length], 0, bytes.length);
            }
            assertThat("read straight from the directory, so nothing is accounted", metrics.instance().getBytesRead(), equalTo(0L));

            // as the store does: the input accounts for itself, so it is handed the holder rather than wrapped
            var storeDirectory = new StoreMetricsDirectory(BlobStoreCacheDirectory.unwrapDirectory(directory), metrics);
            try (IndexInput input = storeDirectory.openInput(FILE_NAME, IOContext.DEFAULT)) {
                assertThat(input, not(instanceOf(StoreMetricsIndexInput.class)));
                input.readBytes(new byte[bytes.length], 0, bytes.length);
            }
            assertThat(metrics.instance().getBytesRead(), equalTo((long) bytes.length));
        }
    }
}
