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

import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.*;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TimeUnits;
import org.elasticsearch.index.store.distributor.Distributor;
import org.elasticsearch.test.ElasticsearchThreadFilter;
import org.elasticsearch.test.junit.listeners.LoggingListener;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

@ThreadLeakFilters(defaultFilters = true, filters = {ElasticsearchThreadFilter.class})
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@TimeoutSuite(millis = 20 * TimeUnits.MINUTE) // timeout the suite after 20min and fail the test.
@Listeners(LoggingListener.class)
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
public class DistributorDirectoryTest extends BaseDirectoryTestCase {

    @Override
    protected Directory getDirectory(Path path) throws IOException {
        Directory[] directories = new Directory[1 + random().nextInt(5)];
        for (int i = 0; i < directories.length; i++) {
            directories[i] = newDirectory();
            if (directories[i] instanceof MockDirectoryWrapper) {
                // TODO: fix this test to handle virus checker
                ((MockDirectoryWrapper) directories[i]).setEnableVirusScanner(false);
            }
        }
        return new DistributorDirectory(directories);
    }

    // #7306: don't invoke the distributor when we are opening an already existing file
    public void testDoNotCallDistributorOnRead() throws Exception {      
        Directory dir = newDirectory();
        dir.createOutput("one.txt", IOContext.DEFAULT).close();

        final Directory[] dirs = new Directory[] {dir};

        Distributor distrib = new Distributor() {

            @Override
            public Directory primary() {
                return dirs[0];
            }

            @Override
            public Directory[] all() {
                return dirs;
            }

            @Override
            public synchronized Directory any() {
                throw new IllegalStateException("any should not be called");
            }
            };

        Directory dd = new DistributorDirectory(distrib);
        assertEquals(0, dd.fileLength("one.txt"));
        dd.openInput("one.txt", IOContext.DEFAULT).close();
        try {
            dd.createOutput("three.txt", IOContext.DEFAULT).close();
            fail("didn't hit expected exception");
        } catch (IllegalStateException ise) {
            // expected
        }
        dd.close();
    }

    public void testTempFilesUsePrimary() throws IOException {
        final int iters = 1 + random().nextInt(10);
        for (int i = 0; i < iters; i++) {
            Directory primary = newDirectory();
            Directory dir = newDirectory();
            final Directory[] dirs = new Directory[] {primary, dir};

            Distributor distrib = new Distributor() {

                @Override
                public Directory primary() {
                    return dirs[0];
                }

                @Override
                public Directory[] all() {
                    return dirs;
                }

                @Override
                public synchronized Directory any() {
                    throw new IllegalStateException("any should not be called");
                }
            };

            DistributorDirectory dd = new DistributorDirectory(distrib);
            String file = RandomPicks.randomFrom(random(), Arrays.asList(Store.CHECKSUMS_PREFIX, IndexFileNames.OLD_SEGMENTS_GEN, IndexFileNames.PENDING_SEGMENTS, IndexFileNames.SEGMENTS));
            String tmpFileName =  "tmp_" + RandomPicks.randomFrom(random(), Arrays.asList("recovery.", "foobar.", "test.")) + Math.max(0, Math.abs(random().nextLong())) + "." + file;
            try (IndexOutput out = dd.createTempOutput(tmpFileName, file, IOContext.DEFAULT)) {
                out.writeInt(1);
            }
            dd.renameFile(tmpFileName, file);
            assertEquals(primary.fileLength(file), 4);
            IOUtils.close(dd);
        }
    }
}
