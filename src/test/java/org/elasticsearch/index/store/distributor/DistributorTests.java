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

package org.elasticsearch.index.store.distributor;

import org.apache.lucene.store.*;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 */
public class DistributorTests extends ElasticsearchTestCase {

    @Test
    public void testLeastUsedDistributor() throws Exception {
        FakeFsDirectory[] directories = new FakeFsDirectory[]{
                new FakeFsDirectory("dir0", 10L),
                new FakeFsDirectory("dir1", 20L),
                new FakeFsDirectory("dir2", 30L)
        };
        FakeDirectoryService directoryService = new FakeDirectoryService(directories);

        LeastUsedDistributor distributor = new LeastUsedDistributor(directoryService);
        for (int i = 0; i < 5; i++) {
            assertThat(distributor.any(), equalTo((Directory) directories[2]));
        }

        directories[2].setUsableSpace(5L);
        for (int i = 0; i < 5; i++) {
            assertThat(distributor.any(), equalTo((Directory) directories[1]));
        }

        directories[1].setUsableSpace(0L);
        for (int i = 0; i < 5; i++) {
            assertThat(distributor.any(), equalTo((Directory) directories[0]));
        }


        directories[0].setUsableSpace(10L);
        directories[1].setUsableSpace(20L);
        directories[2].setUsableSpace(20L);
        for (FakeFsDirectory directory : directories) {
            directory.resetAllocationCount();
        }
        for (int i = 0; i < 10000; i++) {
            ((FakeFsDirectory) distributor.any()).incrementAllocationCount();
        }
        assertThat(directories[0].getAllocationCount(), equalTo(0));
        assertThat((double) directories[1].getAllocationCount() / directories[2].getAllocationCount(), closeTo(1.0, 0.5));

        // Test failover scenario
        for (FakeFsDirectory directory : directories) {
            directory.resetAllocationCount();
        }
        directories[0].setUsableSpace(0L);
        directories[1].setUsableSpace(0L);
        directories[2].setUsableSpace(0L);
        for (int i = 0; i < 10000; i++) {
            ((FakeFsDirectory) distributor.any()).incrementAllocationCount();
        }
        for (FakeFsDirectory directory : directories) {
            assertThat(directory.getAllocationCount(), greaterThan(0));
        }
        assertThat((double) directories[0].getAllocationCount() / directories[2].getAllocationCount(), closeTo(1.0, 0.5));
        assertThat((double) directories[1].getAllocationCount() / directories[2].getAllocationCount(), closeTo(1.0, 0.5));

    }

    @Test
    public void testRandomWeightedDistributor() throws Exception {
        FakeFsDirectory[] directories = new FakeFsDirectory[]{
                new FakeFsDirectory("dir0", 10L),
                new FakeFsDirectory("dir1", 20L),
                new FakeFsDirectory("dir2", 30L)
        };
        FakeDirectoryService directoryService = new FakeDirectoryService(directories);

        RandomWeightedDistributor randomWeightedDistributor = new RandomWeightedDistributor(directoryService);
        for (int i = 0; i < 10000; i++) {
            ((FakeFsDirectory) randomWeightedDistributor.any()).incrementAllocationCount();
        }
        for (FakeFsDirectory directory : directories) {
            assertThat(directory.getAllocationCount(), greaterThan(0));
        }
        assertThat((double) directories[1].getAllocationCount() / directories[0].getAllocationCount(), closeTo(2.0, 0.5));
        assertThat((double) directories[2].getAllocationCount() / directories[0].getAllocationCount(), closeTo(3.0, 0.5));

        for (FakeFsDirectory directory : directories) {
            directory.resetAllocationCount();
        }

        directories[1].setUsableSpace(0L);

        for (int i = 0; i < 1000; i++) {
            ((FakeFsDirectory) randomWeightedDistributor.any()).incrementAllocationCount();
        }

        assertThat(directories[0].getAllocationCount(), greaterThan(0));
        assertThat(directories[1].getAllocationCount(), equalTo(0));
        assertThat(directories[2].getAllocationCount(), greaterThan(0));

    }

    public static class FakeDirectoryService implements DirectoryService {

        private final Directory[] directories;

        public FakeDirectoryService(Directory[] directories) {
            this.directories = directories;
        }

        @Override
        public Directory[] build() throws IOException {
            return directories;
        }

        @Override
        public long throttleTimeInNanos() {
            return 0;
        }

        @Override
        public void renameFile(Directory dir, String from, String to) throws IOException {
        }

        @Override
        public void fullDelete(Directory dir) throws IOException {
        }
    }

    public static class FakeFsDirectory extends FSDirectory {

        public int allocationCount;

        public FakeFile fakeFile;

        public FakeFsDirectory(String path, long usableSpace) throws IOException {
            super(new File(path), NoLockFactory.getNoLockFactory());
            fakeFile = new FakeFile(path, usableSpace);
            allocationCount = 0;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            throw new UnsupportedOperationException("Shouldn't be called in the test");
        }

        public void setUsableSpace(long usableSpace) {
            fakeFile.setUsableSpace(usableSpace);
        }

        public void incrementAllocationCount() {
            allocationCount++;
        }

        public int getAllocationCount() {
            return allocationCount;
        }

        public void resetAllocationCount() {
            allocationCount = 0;
        }

        @Override
        public File getDirectory() {
            return fakeFile;
        }
    }

    public static class FakeFile extends File {
        private long usableSpace;

        public FakeFile(String s, long usableSpace) {
            super(s);
            this.usableSpace = usableSpace;
        }

        @Override
        public long getUsableSpace() {
            return usableSpace;
        }

        public void setUsableSpace(long usableSpace) {
            this.usableSpace = usableSpace;
        }
    }
}
