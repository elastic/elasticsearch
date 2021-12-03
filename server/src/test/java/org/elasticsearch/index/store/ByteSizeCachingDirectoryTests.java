/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class ByteSizeCachingDirectoryTests extends ESTestCase {

    private static class LengthCountingDirectory extends FilterDirectory {

        int numFileLengthCalls;

        LengthCountingDirectory(Directory in) {
            super(in);
        }

        @Override
        public long fileLength(String name) throws IOException {
            numFileLengthCalls++;
            return super.fileLength(name);
        }
    }

    public void testBasics() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("quux", IOContext.DEFAULT)) {
                out.writeBytes(new byte[11], 11);
            }
            LengthCountingDirectory countingDir = new LengthCountingDirectory(dir);

            ByteSizeCachingDirectory cachingDir = new ByteSizeCachingDirectory(countingDir, new TimeValue(0));
            assertEquals(11, cachingDir.estimateSizeInBytes());
            assertEquals(11, cachingDir.estimateSizeInBytes());
            assertEquals(1, countingDir.numFileLengthCalls);

            try (IndexOutput out = cachingDir.createOutput("foo", IOContext.DEFAULT)) {
                out.writeBytes(new byte[5], 5);

                cachingDir.estimateSizeInBytes();
                // +2 because there are 3 files
                assertEquals(3, countingDir.numFileLengthCalls);
                // An index output is open so no caching
                cachingDir.estimateSizeInBytes();
                assertEquals(5, countingDir.numFileLengthCalls);
            }

            assertEquals(16, cachingDir.estimateSizeInBytes());
            assertEquals(7, countingDir.numFileLengthCalls);
            assertEquals(16, cachingDir.estimateSizeInBytes());
            assertEquals(7, countingDir.numFileLengthCalls);

            try (IndexOutput out = cachingDir.createTempOutput("bar", "baz", IOContext.DEFAULT)) {
                out.writeBytes(new byte[4], 4);

                cachingDir.estimateSizeInBytes();
                assertEquals(10, countingDir.numFileLengthCalls);
                // An index output is open so no caching
                cachingDir.estimateSizeInBytes();
                assertEquals(13, countingDir.numFileLengthCalls);
            }

            assertEquals(20, cachingDir.estimateSizeInBytes());
            // +3 because there are 3 files
            assertEquals(16, countingDir.numFileLengthCalls);
            assertEquals(20, cachingDir.estimateSizeInBytes());
            assertEquals(16, countingDir.numFileLengthCalls);

            cachingDir.deleteFile("foo");

            assertEquals(15, cachingDir.estimateSizeInBytes());
            // +2 because there are 2 files now
            assertEquals(18, countingDir.numFileLengthCalls);
            assertEquals(15, cachingDir.estimateSizeInBytes());
            assertEquals(18, countingDir.numFileLengthCalls);
        }
    }

}
