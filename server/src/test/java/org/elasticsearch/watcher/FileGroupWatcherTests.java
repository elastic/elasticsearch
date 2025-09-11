/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.watcher;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class FileGroupWatcherTests extends ESTestCase {

    private static class RecordingFileGroupChangesListener implements FileGroupChangesListener {
        private final List<String> notifications = new ArrayList<>();

        @Override
        public void onFileChanged(List<Path> files) {
            notifications.add("onFileChanged: " + files.size() + " files");
        }

        public List<String> notifications() {
            return notifications;
        }
    }

    public void testMultipleFileChanges() throws IOException {
        Path tempDir = createTempDir();
        RecordingFileGroupChangesListener listener = new RecordingFileGroupChangesListener();
        Path file1 = tempDir.resolve("test1.txt");
        Path file2 = tempDir.resolve("test2.txt");
        Path file3 = tempDir.resolve("test3.txt");

        touch(file1);
        touch(file2);
        touch(file3);

        FileGroupWatcher fileGroupWatcher = new FileGroupWatcher(Arrays.asList(file1, file2, file3));
        fileGroupWatcher.addListener(listener);
        fileGroupWatcher.init();

        // Modify multiple files - should batch into single notification
        Files.setLastModifiedTime(file1, FileTime.fromMillis(System.currentTimeMillis()));
        Files.setLastModifiedTime(file2, FileTime.fromMillis(System.currentTimeMillis()));

        fileGroupWatcher.checkAndNotify();
        assertThat(listener.notifications(), hasSize(1));
        assertThat(listener.notifications().get(0), equalTo("onFileChanged: 2 files"));
    }

    public void testBatchedCreation() throws IOException {
        Path tempDir = createTempDir();
        RecordingFileGroupChangesListener listener = new RecordingFileGroupChangesListener();
        Path file1 = tempDir.resolve("test1.txt");
        Path file2 = tempDir.resolve("test2.txt");

        FileGroupWatcher fileGroupWatcher = new FileGroupWatcher(Arrays.asList(file1, file2));
        fileGroupWatcher.addListener(listener);
        fileGroupWatcher.init();

        // Create both files
        touch(file1);
        touch(file2);

        fileGroupWatcher.checkAndNotify();
        assertThat(listener.notifications(), hasSize(1));
        assertThat(listener.notifications().get(0), equalTo("onFileChanged: 2 files"));
    }

    public void testBatchedDeletion() throws IOException {
        Path tempDir = createTempDir();
        RecordingFileGroupChangesListener listener = new RecordingFileGroupChangesListener();
        Path file1 = tempDir.resolve("test1.txt");
        Path file2 = tempDir.resolve("test2.txt");

        touch(file1);
        touch(file2);

        FileGroupWatcher fileGroupWatcher = new FileGroupWatcher(Arrays.asList(file1, file2));
        fileGroupWatcher.addListener(listener);
        fileGroupWatcher.init();

        Files.delete(file1);
        Files.delete(file2);

        fileGroupWatcher.checkAndNotify();
        assertThat(listener.notifications(), hasSize(1));
        assertThat(listener.notifications().get(0), equalTo("onFileChanged: 2 files"));
    }

    public void testMixedOperationsSeparateNotifications() throws IOException {
        Path tempDir = createTempDir();
        RecordingFileGroupChangesListener listener = new RecordingFileGroupChangesListener();
        Path existingFile = tempDir.resolve("existing.txt");
        Path newFile = tempDir.resolve("new.txt");
        Path deleteFile = tempDir.resolve("delete.txt");

        touch(existingFile);
        touch(deleteFile);

        FileGroupWatcher fileGroupWatcher = new FileGroupWatcher(Arrays.asList(existingFile, newFile, deleteFile));
        fileGroupWatcher.addListener(listener);
        fileGroupWatcher.init();

        Files.setLastModifiedTime(existingFile, FileTime.fromMillis(System.currentTimeMillis()));
        touch(newFile);
        Files.delete(deleteFile);

        fileGroupWatcher.checkAndNotify();
        assertThat(listener.notifications(), hasSize(1));
        assertThat(listener.notifications(), containsInAnyOrder(
            "onFileChanged: 3 files"
        ));
    }

    public void testEmptyFileGroup() throws IOException {
        RecordingFileGroupChangesListener listener = new RecordingFileGroupChangesListener();

        FileGroupWatcher fileGroupWatcher = new FileGroupWatcher(List.of());
        fileGroupWatcher.addListener(listener);
        fileGroupWatcher.init();

        fileGroupWatcher.checkAndNotify();
        assertThat(listener.notifications(), empty());
    }

    static void touch(Path path) throws IOException {
        Files.newOutputStream(path).close();
    }
}
