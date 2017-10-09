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

import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class FileWatcherTests extends ESTestCase {
    private class RecordingChangeListener implements FileChangesListener {
        private Path rootDir;

        private RecordingChangeListener(Path rootDir) {
            this.rootDir = rootDir;
        }

        private String getRelativeFileName(Path file) {
            return rootDir.toUri().relativize(file.toUri()).getPath();
        }

        private List<String> notifications = new ArrayList<>();

        @Override
        public void onFileInit(Path file) {
            notifications.add("onFileInit: " + getRelativeFileName(file));
        }

        @Override
        public void onDirectoryInit(Path file) {
            notifications.add("onDirectoryInit: " + getRelativeFileName(file));
        }

        @Override
        public void onFileCreated(Path file) {
            notifications.add("onFileCreated: " + getRelativeFileName(file));
        }

        @Override
        public void onFileDeleted(Path file) {
            notifications.add("onFileDeleted: " + getRelativeFileName(file));
        }

        @Override
        public void onFileChanged(Path file) {
            notifications.add("onFileChanged: " + getRelativeFileName(file));
        }

        @Override
        public void onDirectoryCreated(Path file) {
            notifications.add("onDirectoryCreated: " + getRelativeFileName(file));
        }

        @Override
        public void onDirectoryDeleted(Path file) {
            notifications.add("onDirectoryDeleted: " + getRelativeFileName(file));
        }

        public List<String> notifications() {
            return notifications;
        }
    }

    public void testSimpleFileOperations() throws IOException {
        Path tempDir = createTempDir();
        RecordingChangeListener changes = new RecordingChangeListener(tempDir);
        Path testFile = tempDir.resolve("test.txt");
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

        Files.delete(testFile);
        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), contains(equalTo("onFileDeleted: test.txt")));

    }

    public void testSimpleDirectoryOperations() throws IOException {
        Path tempDir = createTempDir();
        RecordingChangeListener changes = new RecordingChangeListener(tempDir);
        Path testDir = tempDir.resolve("test-dir");
        Files.createDirectories(testDir);
        touch(testDir.resolve("test.txt"));
        touch(testDir.resolve("test0.txt"));

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
            touch(testDir.resolve("test" + i + ".txt"));
        }
        // Make sure that first file is modified
        append("Test", testDir.resolve("test0.txt"), Charset.defaultCharset());

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

        Files.delete(testDir.resolve("test1.txt"));
        Files.delete(testDir.resolve("test2.txt"));

        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), contains(
                equalTo("onFileDeleted: test-dir/test1.txt"),
                equalTo("onFileDeleted: test-dir/test2.txt")
        ));

        changes.notifications().clear();
        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), hasSize(0));

        Files.delete(testDir.resolve("test0.txt"));
        touch(testDir.resolve("test2.txt"));
        touch(testDir.resolve("test4.txt"));
        fileWatcher.checkAndNotify();

        assertThat(changes.notifications(), contains(
                equalTo("onFileDeleted: test-dir/test0.txt"),
                equalTo("onFileCreated: test-dir/test2.txt"),
                equalTo("onFileCreated: test-dir/test4.txt")
        ));


        changes.notifications().clear();

        Files.delete(testDir.resolve("test3.txt"));
        Files.delete(testDir.resolve("test4.txt"));
        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), contains(
                equalTo("onFileDeleted: test-dir/test3.txt"),
                equalTo("onFileDeleted: test-dir/test4.txt")
        ));


        changes.notifications().clear();
        if (Files.exists(testDir)) {
            IOUtils.rm(testDir);
        }
        fileWatcher.checkAndNotify();

        assertThat(changes.notifications(), contains(
                equalTo("onFileDeleted: test-dir/test.txt"),
                equalTo("onFileDeleted: test-dir/test2.txt"),
                equalTo("onDirectoryDeleted: test-dir")
        ));

    }

    public void testNestedDirectoryOperations() throws IOException {
        Path tempDir = createTempDir();
        RecordingChangeListener changes = new RecordingChangeListener(tempDir);
        Path testDir = tempDir.resolve("test-dir");
        Files.createDirectories(testDir);
        touch(testDir.resolve("test.txt"));
        Files.createDirectories(testDir.resolve("sub-dir"));
        touch(testDir.resolve("sub-dir/test0.txt"));

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
        touch(testDir.resolve("sub-dir/test1.txt"));
        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), contains(
                equalTo("onFileCreated: test-dir/sub-dir/test1.txt")
        ));

        changes.notifications().clear();
        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), hasSize(0));

        // Create new subdirectory in subdirectory
        Files.createDirectories(testDir.resolve("first-level"));
        touch(testDir.resolve("first-level/file1.txt"));
        Files.createDirectories(testDir.resolve("first-level/second-level"));
        touch(testDir.resolve("first-level/second-level/file2.txt"));
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
        Path path = testDir.resolve("first-level");
        if (Files.exists(path)) {
            IOUtils.rm(path);
        }
        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), contains(
                equalTo("onFileDeleted: test-dir/first-level/file1.txt"),
                equalTo("onFileDeleted: test-dir/first-level/second-level/file2.txt"),
                equalTo("onDirectoryDeleted: test-dir/first-level/second-level"),
                equalTo("onDirectoryDeleted: test-dir/first-level")
        ));
    }

    public void testFileReplacingDirectory() throws IOException {
        Path tempDir = createTempDir();
        RecordingChangeListener changes = new RecordingChangeListener(tempDir);
        Path testDir = tempDir.resolve("test-dir");
        Files.createDirectories(testDir);
        Path subDir = testDir.resolve("sub-dir");
        Files.createDirectories(subDir);
        touch(subDir.resolve("test0.txt"));
        touch(subDir.resolve("test1.txt"));

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

        if (Files.exists(subDir)) {
            IOUtils.rm(subDir);
        }
        touch(subDir);
        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), contains(
                equalTo("onFileDeleted: test-dir/sub-dir/test0.txt"),
                equalTo("onFileDeleted: test-dir/sub-dir/test1.txt"),
                equalTo("onDirectoryDeleted: test-dir/sub-dir"),
                equalTo("onFileCreated: test-dir/sub-dir")
        ));

        changes.notifications().clear();

        Files.delete(subDir);
        Files.createDirectories(subDir);

        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), contains(
                equalTo("onFileDeleted: test-dir/sub-dir/"),
                equalTo("onDirectoryCreated: test-dir/sub-dir/")
        ));
    }

    public void testEmptyDirectory() throws IOException {
        Path tempDir = createTempDir();
        RecordingChangeListener changes = new RecordingChangeListener(tempDir);
        Path testDir = tempDir.resolve("test-dir");
        Files.createDirectories(testDir);
        touch(testDir.resolve("test0.txt"));
        touch(testDir.resolve("test1.txt"));

        FileWatcher fileWatcher = new FileWatcher(testDir);
        fileWatcher.addListener(changes);
        fileWatcher.init();
        changes.notifications().clear();

        Files.delete(testDir.resolve("test0.txt"));
        Files.delete(testDir.resolve("test1.txt"));
        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), contains(
                equalTo("onFileDeleted: test-dir/test0.txt"),
                equalTo("onFileDeleted: test-dir/test1.txt")
        ));
    }

    public void testNoDirectoryOnInit() throws IOException {
        Path tempDir = createTempDir();
        RecordingChangeListener changes = new RecordingChangeListener(tempDir);
        Path testDir = tempDir.resolve("test-dir");

        FileWatcher fileWatcher = new FileWatcher(testDir);
        fileWatcher.addListener(changes);
        fileWatcher.init();
        assertThat(changes.notifications(), hasSize(0));
        changes.notifications().clear();

        Files.createDirectories(testDir);
        touch(testDir.resolve("test0.txt"));
        touch(testDir.resolve("test1.txt"));

        fileWatcher.checkAndNotify();
        assertThat(changes.notifications(), contains(
                equalTo("onDirectoryCreated: test-dir/"),
                equalTo("onFileCreated: test-dir/test0.txt"),
                equalTo("onFileCreated: test-dir/test1.txt")
        ));
    }

    public void testNoFileOnInit() throws IOException {
        Path tempDir = createTempDir();
        RecordingChangeListener changes = new RecordingChangeListener(tempDir);
        Path testFile = tempDir.resolve("testfile.txt");

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

    static void touch(Path path) throws IOException {
        Files.newOutputStream(path).close();
    }

    static void append(String string, Path path, Charset cs) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(path, cs, StandardOpenOption.APPEND)) {
            writer.append(string);
        }
    }
}