/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class DelegatingByteSizeDirectoryTests extends ESTestCase {

    public void testUnwrapFindsEachLayerThroughTheChain() throws IOException {
        try (Directory raw = newDirectory()) {
            // the layers as the Store stacks them, under other filters
            FieldInfoCachingDirectory fieldInfos = new FieldInfoCachingDirectory(raw);
            ShardCodecStateDirectory codecState = new ShardCodecStateDirectory(fieldInfos);
            Directory outer = new FilterDirectory(new FilterDirectory(codecState) {}) {};

            assertThat(DelegatingByteSizeDirectory.unwrap(outer, ShardCodecStateDirectory.class), sameInstance(codecState));
            assertThat(DelegatingByteSizeDirectory.unwrap(outer, FieldInfoCachingDirectory.class), sameInstance(fieldInfos));
            assertThat(DelegatingByteSizeDirectory.unwrap(codecState, ShardCodecStateDirectory.class), sameInstance(codecState));
            assertThat(DelegatingByteSizeDirectory.unwrap(fieldInfos, ShardCodecStateDirectory.class), nullValue());
        }
    }

    public void testUnwrapReturnsNullWhenAbsent() throws IOException {
        try (Directory raw = newDirectory()) {
            Directory layered = new FilterDirectory(new FilterDirectory(raw) {}) {};
            assertThat(DelegatingByteSizeDirectory.unwrap(raw, ShardCodecStateDirectory.class), nullValue());
            assertThat(DelegatingByteSizeDirectory.unwrap(layered, ShardCodecStateDirectory.class), nullValue());
        }
    }

    public void testSizeEstimatesDelegateToByteSizeDirectory() throws IOException {
        long size = randomNonNegativeLong();
        long dataSetSize = randomNonNegativeLong();
        try (Directory delegate = new ByteSizeDirectory(newDirectory()) {
            @Override
            public long estimateSizeInBytes() {
                return size;
            }

            @Override
            public long estimateDataSetSizeInBytes() {
                return dataSetSize;
            }
        }) {
            DelegatingByteSizeDirectory directory = new ShardCodecStateDirectory(delegate);
            assertThat(directory.estimateSizeInBytes(), equalTo(size));
            assertThat(directory.estimateDataSetSizeInBytes(), equalTo(dataSetSize));
        }
    }

    public void testSizeEstimatesWalkFilesOfOtherDirectories() throws IOException {
        try (Directory raw = newDirectory()) {
            int files = between(1, 5);
            for (int i = 0; i < files; i++) {
                try (IndexOutput out = raw.createOutput("file" + i, IOContext.DEFAULT)) {
                    byte[] bytes = randomByteArrayOfLength(between(1, 100));
                    out.writeBytes(bytes, bytes.length);
                }
            }
            long expected = 0;
            for (String file : raw.listAll()) {
                expected += raw.fileLength(file);
            }
            assertThat(expected, greaterThan(0L));
            DelegatingByteSizeDirectory directory = new ShardCodecStateDirectory(raw);
            assertThat(directory.estimateSizeInBytes(), equalTo(expected));
            assertThat(directory.estimateDataSetSizeInBytes(), equalTo(expected));
        }
    }
}
