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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.ThreadedIndexingAndSearchingTestCase;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.store.distributor.Distributor;
import org.elasticsearch.test.junit.listeners.LoggingListener;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * This test is a copy of TestNRTThreads from lucene that puts some
 * hard concurrent pressure on the directory etc. to ensure DistributorDirectory is behaving ok.
 */
@LuceneTestCase.SuppressCodecs({ "SimpleText", "Memory", "Direct" })
@ThreadLeakScope(ThreadLeakScope.Scope.SUITE)
@ThreadLeakLingering(linger = 5000) // 5 sec lingering
@Listeners(LoggingListener.class)
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
public class DistributorInTheWildTest extends ThreadedIndexingAndSearchingTestCase {
    protected final ESLogger logger = Loggers.getLogger(getClass());

    private boolean useNonNrtReaders = true;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        useNonNrtReaders  = random().nextBoolean();
    }

    @Override
    protected void doSearching(ExecutorService es, long stopTime) throws Exception {

        boolean anyOpenDelFiles = false;

        DirectoryReader r = DirectoryReader.open(writer, true);

        while (System.currentTimeMillis() < stopTime && !failed.get()) {
            if (random().nextBoolean()) {
                if (VERBOSE) {
                    logger.info("TEST: now reopen r=" + r);
                }
                final DirectoryReader r2 = DirectoryReader.openIfChanged(r);
                if (r2 != null) {
                    r.close();
                    r = r2;
                }
            } else {
                if (VERBOSE) {
                    logger.info("TEST: now close reader=" + r);
                }
                r.close();
                writer.commit();
                final Set<String> openDeletedFiles = getOpenDeletedFiles(dir);
                if (openDeletedFiles.size() > 0) {
                    logger.info("OBD files: " + openDeletedFiles);
                }
                anyOpenDelFiles |= openDeletedFiles.size() > 0;
                //assertEquals("open but deleted: " + openDeletedFiles, 0, openDeletedFiles.size());
                if (VERBOSE) {
                    logger.info("TEST: now open");
                }
                r = DirectoryReader.open(writer, true);
            }
            if (VERBOSE) {
                logger.info("TEST: got new reader=" + r);
            }
            //logger.info("numDocs=" + r.numDocs() + "
            //openDelFileCount=" + dir.openDeleteFileCount());

            if (r.numDocs() > 0) {
                fixedSearcher = new IndexSearcher(r, es);
                smokeTestSearcher(fixedSearcher);
                runSearchThreads(System.currentTimeMillis() + 500);
            }
        }
        r.close();

        //logger.info("numDocs=" + r.numDocs() + " openDelFileCount=" + dir.openDeleteFileCount());
        final Set<String> openDeletedFiles = getOpenDeletedFiles(dir);
        if (openDeletedFiles.size() > 0) {
            logger.info("OBD files: " + openDeletedFiles);
        }
        anyOpenDelFiles |= openDeletedFiles.size() > 0;

        assertFalse("saw non-zero open-but-deleted count", anyOpenDelFiles);
    }

    private Set<String> getOpenDeletedFiles(Directory dir) throws IOException {
        if (random().nextBoolean() && dir instanceof  MockDirectoryWrapper) {
            return ((MockDirectoryWrapper) dir).getOpenDeletedFiles();
        }
        DistributorDirectory d = DirectoryUtils.getLeaf(dir, DistributorDirectory.class, null);
        Distributor distributor = d.getDistributor();
        Set<String> set = new HashSet<>();
        for (Directory subDir : distributor.all()) {
            Set<String> openDeletedFiles = ((MockDirectoryWrapper) subDir).getOpenDeletedFiles();
            set.addAll(openDeletedFiles);
        }
        return set;
    }

    @Override
    protected Directory getDirectory(Directory in) {
        assert in instanceof MockDirectoryWrapper;
        if (!useNonNrtReaders) ((MockDirectoryWrapper) in).setAssertNoDeleteOpenFile(true);

        Directory[] directories = new Directory[1 + random().nextInt(5)];
        directories[0] = in;
        for (int i = 1; i < directories.length; i++) {
            final File tempDir = createTempDir(getTestName());
            directories[i] = newMockFSDirectory(tempDir); // some subclasses rely on this being MDW
            if (!useNonNrtReaders) ((MockDirectoryWrapper) directories[i]).setAssertNoDeleteOpenFile(true);
        }
        for (Directory dir : directories) {
            ((MockDirectoryWrapper) dir).setCheckIndexOnClose(false);
        }

        try {

            if (random().nextBoolean()) {
                return new MockDirectoryWrapper(random(), new DistributorDirectory(directories));
            } else {
                return new DistributorDirectory(directories);
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    protected void doAfterWriter(ExecutorService es) throws Exception {
        // Force writer to do reader pooling, always, so that
        // all merged segments, even for merges before
        // doSearching is called, are warmed:
        DirectoryReader.open(writer, true).close();
    }

    private IndexSearcher fixedSearcher;

    @Override
    protected IndexSearcher getCurrentSearcher() throws Exception {
        return fixedSearcher;
    }

    @Override
    protected void releaseSearcher(IndexSearcher s) throws Exception {
        if (s != fixedSearcher) {
            // Final searcher:
            s.getIndexReader().close();
        }
    }

    @Override
    protected IndexSearcher getFinalSearcher() throws Exception {
        final IndexReader r2;
        if (useNonNrtReaders) {
            if (random().nextBoolean()) {
                r2 = DirectoryReader.open(writer, true);
            } else {
                writer.commit();
                r2 = DirectoryReader.open(dir);
            }
        } else {
            r2 = DirectoryReader.open(writer, true);
        }
        return newSearcher(r2);
    }

    public void testNRTThreads() throws Exception {
        runTest("TestNRTThreads");
    }
}
