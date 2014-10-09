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

import java.io.File;
import java.io.IOException;

import org.apache.lucene.store.BaseDirectoryTestCase;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TimeUnits;
import org.elasticsearch.index.store.distributor.Distributor;
import org.elasticsearch.test.ElasticsearchThreadFilter;
import org.elasticsearch.test.junit.listeners.LoggingListener;
import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

@ThreadLeakFilters(defaultFilters = true, filters = {ElasticsearchThreadFilter.class})
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@TimeoutSuite(millis = 20 * TimeUnits.MINUTE) // timeout the suite after 20min and fail the test.
@Listeners(LoggingListener.class)
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
public class DistributorDirectoryTest extends BaseDirectoryTestCase {

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
}
