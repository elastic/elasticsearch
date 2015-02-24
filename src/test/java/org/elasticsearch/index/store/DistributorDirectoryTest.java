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
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.BaseDirectoryTestCase;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TimeUnits;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.distributor.Distributor;
import org.elasticsearch.test.junit.listeners.LoggingListener;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;

@ThreadLeakScope(ThreadLeakScope.Scope.SUITE)
@ThreadLeakLingering(linger = 5000) // 5 sec lingering
@TimeoutSuite(millis = 5 * TimeUnits.MINUTE)
@Listeners(LoggingListener.class)
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
public class DistributorDirectoryTest extends BaseDirectoryTestCase {
    protected final ESLogger logger = Loggers.getLogger(getClass());

    @Override
    protected Directory getDirectory(File path) throws IOException {
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

        DistributorDirectory dd = new DistributorDirectory(distrib);
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

    public void testRenameFiles() throws IOException {
        final int iters = 1 + random().nextInt(10);
        for (int i = 0; i < iters; i++) {
            Directory[] dirs = new Directory[1 + random().nextInt(5)];
            for (int j=0; j < dirs.length; j++) {
                MockDirectoryWrapper directory  = newMockDirectory();
                directory.setEnableVirusScanner(false);
                directory.setCheckIndexOnClose(false);
                dirs[j] = directory;
            }

            DistributorDirectory dd = new DistributorDirectory(dirs);
            String file = RandomPicks.randomFrom(random(), Arrays.asList(Store.CHECKSUMS_PREFIX, IndexFileNames.SEGMENTS_GEN));
            String tmpFileName =  RandomPicks.randomFrom(random(), Arrays.asList("recovery.", "foobar.", "test.")) + Math.max(0, Math.abs(random().nextLong())) + "." + file;
            try (IndexOutput out = dd.createOutput(tmpFileName, IOContext.DEFAULT)) {
                out.writeInt(1);
            }
            Directory theDir = null;
            for (Directory d : dirs) {
                try {
                    if (d.fileLength(tmpFileName) > 0) {
                        theDir = d;
                        break;
                    }
                } catch (IOException ex) {
                    // nevermind
                }
            }
            assertNotNull("file must be in at least one dir", theDir);
            DirectoryService service = new DirectoryService(new ShardId("foo", 1), ImmutableSettings.EMPTY) {
                @Override
                public Directory[] build() throws IOException {
                    return new Directory[0];
                }

                @Override
                public long throttleTimeInNanos() {
                    return 0;
                }

                @Override
                public void renameFile(Directory dir, String from, String to) throws IOException {
                    dir.copy(dir, from, to, IOContext.DEFAULT);
                    dir.deleteFile(from);
                }

            };
            dd.renameFile(service, tmpFileName, file);
            try {
                dd.fileLength(tmpFileName);
                fail("file ["+tmpFileName + "] was renamed but still exists");
            } catch (FileNotFoundException | NoSuchFileException ex) {
                // all is well
            }
            try {
                theDir.fileLength(tmpFileName);
                fail("file ["+tmpFileName + "] was renamed but still exists");
            } catch (FileNotFoundException | NoSuchFileException ex) {
                // all is well
            }


            assertEquals(theDir.fileLength(file), 4);

            try (IndexOutput out = dd.createOutput("foo.bar", IOContext.DEFAULT)) {
                out.writeInt(1);
            }
            assertNotNull(dd);
            if (dd.getDirectory("foo.bar") != dd.getDirectory(file)) {
                try {
                    dd.renameFile(service, "foo.bar", file);
                    fail("target file already exists in a different directory");
                } catch (IOException ex) {
                    // target file already exists
                }
            }
            IOUtils.close(dd);
        }
    }
}
