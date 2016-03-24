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

import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FileSwitchDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;

public class DirectoryUtilsTests extends ESTestCase {
    public void testGetLeave() throws IOException {
        Path file = createTempDir();
        final int iters = scaledRandomIntBetween(10, 100);
        for (int i = 0; i < iters; i++) {
            {
                BaseDirectoryWrapper dir = newFSDirectory(file);
                FSDirectory directory = DirectoryUtils.getLeaf(new FilterDirectory(dir) {}, FSDirectory.class, null);
                assertThat(directory, notNullValue());
                assertThat(directory, sameInstance(DirectoryUtils.getLeafDirectory(dir, null)));
                dir.close();
            }

            {
                BaseDirectoryWrapper dir = newFSDirectory(file);
                FSDirectory directory = DirectoryUtils.getLeaf(dir, FSDirectory.class, null);
                assertThat(directory, notNullValue());
                assertThat(directory, sameInstance(DirectoryUtils.getLeafDirectory(dir, null)));
                dir.close();
            }

            {
                Set<String> stringSet = Collections.emptySet();
                BaseDirectoryWrapper dir = newFSDirectory(file);
                FSDirectory directory = DirectoryUtils.getLeaf(new FileSwitchDirectory(stringSet, dir, dir, random().nextBoolean()), FSDirectory.class, null);
                assertThat(directory, notNullValue());
                assertThat(directory, sameInstance(DirectoryUtils.getLeafDirectory(dir, null)));
                dir.close();
            }

            {
                Set<String> stringSet = Collections.emptySet();
                BaseDirectoryWrapper dir = newFSDirectory(file);
                FSDirectory directory = DirectoryUtils.getLeaf(new FilterDirectory(new FileSwitchDirectory(stringSet, dir, dir, random().nextBoolean())) {}, FSDirectory.class, null);
                assertThat(directory, notNullValue());
                assertThat(directory, sameInstance(DirectoryUtils.getLeafDirectory(dir, null)));
                dir.close();
            }

            {
                Set<String> stringSet = Collections.emptySet();
                BaseDirectoryWrapper dir = newFSDirectory(file);
                RAMDirectory directory = DirectoryUtils.getLeaf(new FilterDirectory(new FileSwitchDirectory(stringSet, dir, dir, random().nextBoolean())) {}, RAMDirectory.class, null);
                assertThat(directory, nullValue());
                dir.close();
            }

        }
    }
}
