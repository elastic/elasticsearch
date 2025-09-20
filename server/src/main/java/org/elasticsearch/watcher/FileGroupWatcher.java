/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.watcher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * An abstraction that coalesces any number of updates, deletes or creations for any number of files into ONE update
 * per configured Resource Watcher period. To this end, the semantics are such that distinctions between
 * updates, deletes or create actions are all collapsed and everything is referred to as a "change".
 * <p>
 * The implementation relies on FileWatcher for the specifics of monitoring files. This class acts like an
 * orchestrator, initiating and reacting to the results of a collection of file watchers.
 */
public class FileGroupWatcher extends AbstractResourceWatcher<FileGroupChangesListener> {

    private static final Logger logger = LogManager.getLogger(FileGroupWatcher.class);

    private final List<FileWatcher> watchedFiles;
    private final CoalescingFileChangesListener coalescingListener;

    /**
     * @param files a group of files to be watched
     */
    public FileGroupWatcher(Collection<Path> files) {
        this.coalescingListener = new CoalescingFileChangesListener();
        this.watchedFiles = new ArrayList<>();
        for (Path file : files) {
            final FileWatcher watcher = new FileWatcher(file);
            watcher.addListener(coalescingListener);
            watchedFiles.add(watcher);
        }
    }

    @Override
    protected void doInit() throws IOException {
        for (FileWatcher watcher : watchedFiles) {
            watcher.doInit();
        }
    }

    @Override
    protected void doCheckAndNotify() throws IOException {
        if (coalescingListener.filesChanged()) {
            // fallback in case state wasn't cleared correctly in previous iteration
            logger.error("Detected inconsistent state in the File Group Watcher, clearing it and proceeding.");
            coalescingListener.reset();
        }
        for (FileWatcher watcher : watchedFiles) {
            watcher.doCheckAndNotify();
        }
        // at this point the results of all file changes has coalesced
        if (coalescingListener.filesChanged()) {
            logger.info("Number of changed files: {}", coalescingListener.getFilesChangedCount());
            try {
                for (FileGroupChangesListener listener : listeners()) {
                    try {
                        listener.onFileChanged(coalescingListener.getFiles());
                    } catch (Exception e) {
                        logger.warn("Cannot notify file group changes listener {}", listener.getClass().getSimpleName(), e);
                    }
                }
            } finally {
                coalescingListener.reset();
            }
        }
    }

    static class CoalescingFileChangesListener implements FileChangesListener {

        private final ThreadLocal<List<Path>> filesChanged = ThreadLocal.withInitial(ArrayList::new);

        @Override
        public void onFileChanged(Path file) {
            filesChanged.get().add(file);
        }

        @Override
        public void onFileCreated(Path file) {
            onFileChanged(file);
        }

        @Override
        public void onFileDeleted(Path file) {
            onFileChanged(file);
        }

        public boolean filesChanged() {
            return filesChanged.get().isEmpty() == false;
        }

        public int getFilesChangedCount() {
            return filesChanged.get().size();
        }

        public void reset() {
            filesChanged.set(new ArrayList<>());
        }

        public List<Path> getFiles() {
            return new ArrayList<>(filesChanged.get());
        }

    }
}
