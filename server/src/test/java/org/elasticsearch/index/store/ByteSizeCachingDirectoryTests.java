/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

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
            assertEquals(11, cachingDir.estimateSize());
            assertEquals(11, cachingDir.estimateSize());
            assertEquals(1, countingDir.numFileLengthCalls);

            try (IndexOutput out = cachingDir.createOutput("foo", IOContext.DEFAULT)) {
                out.writeBytes(new byte[5], 5);
            }

            assertEquals(16, cachingDir.estimateSize());
            assertEquals(3, countingDir.numFileLengthCalls);
            assertEquals(16, cachingDir.estimateSize());
            assertEquals(3, countingDir.numFileLengthCalls);

            try (IndexOutput out = cachingDir.createTempOutput("bar", "baz", IOContext.DEFAULT)) {
                out.writeBytes(new byte[4], 4);
            }

            assertEquals(20, cachingDir.estimateSize());
            // +3 because there are 3 files
            assertEquals(6, countingDir.numFileLengthCalls);
            assertEquals(20, cachingDir.estimateSize());
            assertEquals(6, countingDir.numFileLengthCalls);
        }
    }

}
