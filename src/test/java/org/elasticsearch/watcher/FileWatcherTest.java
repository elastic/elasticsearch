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
package org.elasticsearch.watcher;

import com.carrotsearch.randomizedtesting.LifecycleScope;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.io.Files.*;
import static org.elasticsearch.common.io.FileSystemUtils.deleteRecursively;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class FileWatcherTest extends ElasticsearchTestCase {

    private class RecordingChangeListener extends FileChangesListener {

        private File rootDir;

        private RecordingChangeListener(File rootDir) {
            this.rootDir = rootDir;
        }

        private String getRelativeFileName(File file) {
            return rootDir.toURI().relativize(file.toURI()).getPath();
        }

        private List<String> notifications = newArrayList();

        @Override
        public void onFileInit(File file) {
            notifications.add("onFileInit: " + getRelativeFileName(file));
        }

        @Override
        public void onDirectoryInit(File file) {
            notifications.add("onDirectoryInit: " + getRelativeFileName(file));
        }

        @Override
        public void onFileCreated(File file) {
            notifications.add("onFileCreated: " + getRelativeFileName(file));
        }

        @Override
        public void onFileDeleted(File file) {
            notifications.add("onFileDeleted: " + getRelativeFileName(file));
        }

        @Override
        public void onFileChanged(File file) {
            notifications.add("onFileChanged: " + getRelativeFileName(file));
        }

        @Override
        public void onDirectoryCreated(File file) {
            notifications.add("onDirectoryCreated: " + getRelativeFileName(file));
        }

        @Override
        public void onDirectoryDeleted(File file) {
            notifications.add("onDirectoryDeleted: " + getRelativeFileName(file));
        }

        public List<String> notifications() {
            return notifications;
        }
    }

    @Test
    public void testSimpleFileOperations() throws IOException {
        File tempDir = newTempDir(LifecycleScope.TEST);
        RecordingChangeListener changes = new RecordingChangeListener(tempDir);
        File testFile = new File(tempDir, "test.txt");
        touch(testFile);
        FileWatcher fileWatcher = new FileWatcher(testFile);
        fileWatcher.addListener(changes);
        fileWatcher.init();
        assertThat(changes.notifications(), contains(equalTo("onFileInit: test.txt")));

        changes.notifications().clear();
        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), hasSize(0));

        append("Test", testFile, Charset.defaultCharset());
        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), contains(equalTo("onFileChanged: test.txt")));

        changes.notifications().clear();
        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), hasSize(0));

        testFile.delete();
        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), contains(equalTo("onFileDeleted: test.txt")));

    }

    @Test
    public void testSimpleDirectoryOperations() throws IOException {
        File tempDir = newTempDir(LifecycleScope.TEST);
        RecordingChangeListener changes = new RecordingChangeListener(tempDir);
        File testDir = new File(tempDir, "test-dir");
        testDir.mkdir();
        touch(new File(testDir, "test.txt"));
        touch(new File(testDir, "test0.txt"));

        FileWatcher fileWatcher = new FileWatcher(testDir);
        fileWatcher.addListener(changes);
        fileWatcher.init();
        assertThat(changes.notifications(), contains(
                equalTo("onDirectoryInit: test-dir/"),
                equalTo("onFileInit: test-dir/test.txt"),
                equalTo("onFileInit: test-dir/test0.txt")
        ));

        changes.notifications().clear();
        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), hasSize(0));

        for (int i = 0; i < 4; i++) {
            touch(new File(testDir, "test" + i + ".txt"));
        }
        // Make sure that first file is modified
        append("Test", new File(testDir, "test0.txt"), Charset.defaultCharset());

        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), contains(
                equalTo("onFileChanged: test-dir/test0.txt"),
                equalTo("onFileCreated: test-dir/test1.txt"),
                equalTo("onFileCreated: test-dir/test2.txt"),
                equalTo("onFileCreated: test-dir/test3.txt")
        ));

        changes.notifications().clear();
        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), hasSize(0));

        new File(testDir, "test1.txt").delete();
        new File(testDir, "test2.txt").delete();

        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), contains(
                equalTo("onFileDeleted: test-dir/test1.txt"),
                equalTo("onFileDeleted: test-dir/test2.txt")
        ));

        changes.notifications().clear();
        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), hasSize(0));

        new File(testDir, "test0.txt").delete();
        touch(new File(testDir, "test2.txt"));
        touch(new File(testDir, "test4.txt"));
        fileWatcher.checkAndNotify();

        assertThat(changes.notifications(), contains(
                equalTo("onFileDeleted: test-dir/test0.txt"),
                equalTo("onFileCreated: test-dir/test2.txt"),
                equalTo("onFileCreated: test-dir/test4.txt")
        ));


        changes.notifications().clear();

        new File(testDir, "test3.txt").delete();
        new File(testDir, "test4.txt").delete();
        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), contains(
                equalTo("onFileDeleted: test-dir/test3.txt"),
                equalTo("onFileDeleted: test-dir/test4.txt")
        ));


        changes.notifications().clear();
        deleteRecursively(testDir);
        fileWatcher.checkAndNotify();

        assertThat(changes.notifications(), contains(
                equalTo("onFileDeleted: test-dir/test.txt"),
                equalTo("onFileDeleted: test-dir/test2.txt"),
                equalTo("onDirectoryDeleted: test-dir")
        ));

    }

    @Test
    public void testNestedDirectoryOperations() throws IOException {
        File tempDir = newTempDir(LifecycleScope.TEST);
        RecordingChangeListener changes = new RecordingChangeListener(tempDir);
        File testDir = new File(tempDir, "test-dir");
        testDir.mkdir();
        touch(new File(testDir, "test.txt"));
        new File(testDir, "sub-dir").mkdir();
        touch(new File(testDir, "sub-dir/test0.txt"));

        FileWatcher fileWatcher = new FileWatcher(testDir);
        fileWatcher.addListener(changes);
        fileWatcher.init();
        assertThat(changes.notifications(), contains(
                equalTo("onDirectoryInit: test-dir/"),
                equalTo("onDirectoryInit: test-dir/sub-dir/"),
                equalTo("onFileInit: test-dir/sub-dir/test0.txt"),
                equalTo("onFileInit: test-dir/test.txt")
        ));

        changes.notifications().clear();
        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), hasSize(0));

        // Create new file in subdirectory
        touch(new File(testDir, "sub-dir/test1.txt"));
        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), contains(
                equalTo("onFileCreated: test-dir/sub-dir/test1.txt")
        ));

        changes.notifications().clear();
        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), hasSize(0));

        // Create new subdirectory in subdirectory
        new File(testDir, "first-level").mkdir();
        touch(new File(testDir, "first-level/file1.txt"));
        new File(testDir, "first-level/second-level").mkdir();
        touch(new File(testDir, "first-level/second-level/file2.txt"));
        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), contains(
                equalTo("onDirectoryCreated: test-dir/first-level/"),
                equalTo("onFileCreated: test-dir/first-level/file1.txt"),
                equalTo("onDirectoryCreated: test-dir/first-level/second-level/"),
                equalTo("onFileCreated: test-dir/first-level/second-level/file2.txt")
        ));

        changes.notifications().clear();
        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), hasSize(0));

        // Delete a directory, check notifications for
        deleteRecursively(new File(testDir, "first-level"));
        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), contains(
                equalTo("onFileDeleted: test-dir/first-level/file1.txt"),
                equalTo("onFileDeleted: test-dir/first-level/second-level/file2.txt"),
                equalTo("onDirectoryDeleted: test-dir/first-level/second-level"),
                equalTo("onDirectoryDeleted: test-dir/first-level")
        ));
    }

    @Test
    public void testFileReplacingDirectory() throws IOException {
        File tempDir = newTempDir(LifecycleScope.TEST);
        RecordingChangeListener changes = new RecordingChangeListener(tempDir);
        File testDir = new File(tempDir, "test-dir");
        testDir.mkdir();
        File subDir = new File(testDir, "sub-dir");
        subDir.mkdir();
        touch(new File(subDir, "test0.txt"));
        touch(new File(subDir, "test1.txt"));

        FileWatcher fileWatcher = new FileWatcher(testDir);
        fileWatcher.addListener(changes);
        fileWatcher.init();
        assertThat(changes.notifications(), contains(
                equalTo("onDirectoryInit: test-dir/"),
                equalTo("onDirectoryInit: test-dir/sub-dir/"),
                equalTo("onFileInit: test-dir/sub-dir/test0.txt"),
                equalTo("onFileInit: test-dir/sub-dir/test1.txt")
        ));

        changes.notifications().clear();

        deleteRecursively(subDir);
        touch(subDir);
        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), contains(
                equalTo("onFileDeleted: test-dir/sub-dir/test0.txt"),
                equalTo("onFileDeleted: test-dir/sub-dir/test1.txt"),
                equalTo("onDirectoryDeleted: test-dir/sub-dir"),
                equalTo("onFileCreated: test-dir/sub-dir")
        ));

        changes.notifications().clear();

        subDir.delete();
        subDir.mkdir();

        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), contains(
                equalTo("onFileDeleted: test-dir/sub-dir/"),
                equalTo("onDirectoryCreated: test-dir/sub-dir/")
        ));
    }

    @Test
    public void testEmptyDirectory() throws IOException {
        File tempDir = newTempDir(LifecycleScope.TEST);
        RecordingChangeListener changes = new RecordingChangeListener(tempDir);
        File testDir = new File(tempDir, "test-dir");
        testDir.mkdir();
        touch(new File(testDir, "test0.txt"));
        touch(new File(testDir, "test1.txt"));

        FileWatcher fileWatcher = new FileWatcher(testDir);
        fileWatcher.addListener(changes);
        fileWatcher.init();
        changes.notifications().clear();

        new File(testDir, "test0.txt").delete();
        new File(testDir, "test1.txt").delete();
        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), contains(
                equalTo("onFileDeleted: test-dir/test0.txt"),
                equalTo("onFileDeleted: test-dir/test1.txt")
        ));
    }

    @Test
    public void testNoDirectoryOnInit() throws IOException {
        File tempDir = newTempDir(LifecycleScope.TEST);
        RecordingChangeListener changes = new RecordingChangeListener(tempDir);
        File testDir = new File(tempDir, "test-dir");

        FileWatcher fileWatcher = new FileWatcher(testDir);
        fileWatcher.addListener(changes);
        fileWatcher.init();
        assertThat(changes.notifications(), hasSize(0));
        changes.notifications().clear();

        testDir.mkdir();
        touch(new File(testDir, "test0.txt"));
        touch(new File(testDir, "test1.txt"));

        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), contains(
                equalTo("onDirectoryCreated: test-dir/"),
                equalTo("onFileCreated: test-dir/test0.txt"),
                equalTo("onFileCreated: test-dir/test1.txt")
        ));
    }

    @Test
    public void testNoFileOnInit() throws IOException {
        File tempDir = newTempDir(LifecycleScope.TEST);
        RecordingChangeListener changes = new RecordingChangeListener(tempDir);
        File testFile = new File(tempDir, "testfile.txt");

        FileWatcher fileWatcher = new FileWatcher(testFile);
        fileWatcher.addListener(changes);
        fileWatcher.init();
        assertThat(changes.notifications(), hasSize(0));
        changes.notifications().clear();

        touch(testFile);

        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), contains(
                equalTo("onFileCreated: testfile.txt")
        ));
    }

}