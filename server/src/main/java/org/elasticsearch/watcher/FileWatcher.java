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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.util.CollectionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.stream.StreamSupport;

/**
 * File resources watcher
 *
 * The file watcher checks directory and all its subdirectories for file changes and notifies its listeners accordingly
 */
public class FileWatcher extends AbstractResourceWatcher<FileChangesListener> {

    private FileObserver rootFileObserver;
    private final Path path;
    private final boolean checkFileContents;

    private static final Logger logger = LogManager.getLogger(FileWatcher.class);

    /**
     * Creates new file watcher on the given directory
     * @param path the directory to watch
     */
    public FileWatcher(Path path) {
        this(path, false);
    }

    /**
     * Creates new file watcher on the given directory
     * @param path the directory to watch
     * @param checkFileContents whether to inspect the content of the file for changes (via a message digest)
     *                          - this is a "best efforts" check and will err on the side of sending extra change notifications if the file
     *                          <em>might</em> have changed.
     */
    public FileWatcher(Path path, boolean checkFileContents) {
        this.path = path;
        this.checkFileContents = checkFileContents;
        rootFileObserver = new FileObserver(path);
    }

    /**
     * Clears any state with the FileWatcher, making all files show up as new
     */
    public void clearState() {
        rootFileObserver = new FileObserver(path);
        try {
            rootFileObserver.init(false);
        } catch (IOException e) {
            // ignore IOException
        }
    }

    @Override
    protected void doInit() throws IOException {
        rootFileObserver.init(true);
    }

    @Override
    protected void doCheckAndNotify() throws IOException {
        rootFileObserver.checkAndNotify();
    }

    private static final Observer[] EMPTY_DIRECTORY = new Observer[0];

    private abstract static class Observer {
        final Path path;
        boolean exists;
        boolean isDirectory;

        private Observer(Path path) {
            this.path = path;
        }

        abstract void checkAndNotify() throws IOException;

        abstract void onDirectoryDeleted();

        abstract void onFileDeleted();
    }

    /**
     * A placeholder {@link Observer} for a file that we don't have permissions to access.
     * We can't watch it for changes, but it shouldn't block us from watching other files in the same directory.
     */
    private static class DeniedObserver extends Observer {
        private DeniedObserver(Path path) {
            super(path);
        }

        @Override
        void checkAndNotify() throws IOException {}

        @Override
        void onDirectoryDeleted() {}

        @Override
        void onFileDeleted() {}
    }

    protected boolean fileExists(Path path) {
        return Files.exists(path);
    }

    protected BasicFileAttributes readAttributes(Path path) throws IOException {
        return Files.readAttributes(path, BasicFileAttributes.class);
    }

    protected InputStream newInputStream(Path path) throws IOException {
        return Files.newInputStream(path);
    }

    protected DirectoryStream<Path> listFiles(Path path) throws IOException {
        return Files.newDirectoryStream(path);
    }

    private class FileObserver extends Observer {
        private long length;
        private long lastModified;
        private Observer[] children;
        private byte[] digest;

        FileObserver(Path path) {
            super(path);
        }

        public void checkAndNotify() throws IOException {
            boolean prevExists = exists;
            boolean prevIsDirectory = isDirectory;
            long prevLength = length;
            long prevLastModified = lastModified;
            byte[] prevDigest = digest;

            exists = fileExists(path);
            // TODO we might use the new NIO2 API to get real notification?
            if (exists) {
                BasicFileAttributes attributes = readAttributes(path);
                isDirectory = attributes.isDirectory();
                if (isDirectory) {
                    length = 0;
                    lastModified = 0;
                } else {
                    length = attributes.size();
                    lastModified = attributes.lastModifiedTime().toMillis();
                }
            } else {
                isDirectory = false;
                length = 0;
                lastModified = 0;
            }

            // Perform notifications and update children for the current file
            if (prevExists) {
                if (exists) {
                    if (isDirectory) {
                        if (prevIsDirectory) {
                            // Remained a directory
                            updateChildren();
                        } else {
                            // File replaced by directory
                            onFileDeleted();
                            onDirectoryCreated(false);
                        }
                    } else {
                        if (prevIsDirectory) {
                            // Directory replaced by file
                            onDirectoryDeleted();
                            onFileCreated(false);
                        } else {
                            // Remained file
                            if (prevLastModified != lastModified || prevLength != length) {
                                if (checkFileContents) {
                                    digest = calculateDigest();
                                    if (digest == null || Arrays.equals(prevDigest, digest) == false) {
                                        onFileChanged();
                                    }
                                } else {
                                    onFileChanged();
                                }
                            }
                        }
                    }
                } else {
                    // Deleted
                    if (prevIsDirectory) {
                        onDirectoryDeleted();
                    } else {
                        onFileDeleted();
                    }
                }
            } else {
                // Created
                if (exists) {
                    if (isDirectory) {
                        onDirectoryCreated(false);
                    } else {
                        onFileCreated(false);
                    }
                }
            }

        }

        private byte[] calculateDigest() {
            try (var in = newInputStream(path)) {
                return MessageDigests.digest(in, MessageDigests.md5());
            } catch (IOException e) {
                logger.warn(
                    "failed to read file [{}] while checking for file changes [{}], will assuming file has been modified",
                    path,
                    e.toString()
                );
                return null;
            }
        }

        private void init(boolean initial) throws IOException {
            exists = fileExists(path);
            if (exists) {
                BasicFileAttributes attributes = readAttributes(path);
                isDirectory = attributes.isDirectory();
                if (isDirectory) {
                    onDirectoryCreated(initial);
                } else {
                    length = attributes.size();
                    lastModified = attributes.lastModifiedTime().toMillis();
                    if (checkFileContents) {
                        digest = calculateDigest();
                    }
                    onFileCreated(initial);
                }
            }
        }

        private Observer createChild(Path file, boolean initial) throws IOException {
            try {
                FileObserver child = new FileObserver(file);
                child.init(initial);
                return child;
            } catch (SecurityException e) {
                // don't have permissions, use a placeholder
                logger.debug(() -> Strings.format("Don't have permissions to watch path [%s]", file), e);
                return new DeniedObserver(file);
            }
        }

        private Path[] listFiles() throws IOException {
            try (var dirs = FileWatcher.this.listFiles(path)) {
                return StreamSupport.stream(dirs.spliterator(), false).sorted().toArray(Path[]::new);
            }
        }

        private Observer[] listChildren(boolean initial) throws IOException {
            Path[] files = listFiles();
            if (CollectionUtils.isEmpty(files) == false) {
                Observer[] childObservers = new Observer[files.length];
                for (int i = 0; i < files.length; i++) {
                    childObservers[i] = createChild(files[i], initial);
                }
                return childObservers;
            } else {
                return EMPTY_DIRECTORY;
            }
        }

        private void updateChildren() throws IOException {
            Path[] files = listFiles();
            if (CollectionUtils.isEmpty(files) == false) {
                Observer[] newChildren = new Observer[files.length];
                int child = 0;
                int file = 0;
                while (file < files.length || child < children.length) {
                    int compare;

                    if (file >= files.length) {
                        compare = -1;
                    } else if (child >= children.length) {
                        compare = 1;
                    } else {
                        compare = children[child].path.compareTo(files[file]);
                    }

                    if (compare == 0) {
                        // Same file copy it and update
                        children[child].checkAndNotify();
                        newChildren[file] = children[child];
                        file++;
                        child++;
                    } else {
                        if (compare > 0) {
                            // This child doesn't appear in the old list - init it
                            newChildren[file] = createChild(files[file], false);
                            file++;
                        } else {
                            // The child from the old list is missing in the new list
                            // Delete it
                            deleteChild(child);
                            child++;
                        }
                    }
                }
                children = newChildren;
            } else {
                // No files - delete all children
                for (int child = 0; child < children.length; child++) {
                    deleteChild(child);
                }
                children = EMPTY_DIRECTORY;
            }
        }

        private void deleteChild(int child) {
            if (children[child].exists) {
                if (children[child].isDirectory) {
                    children[child].onDirectoryDeleted();
                } else {
                    children[child].onFileDeleted();
                }
            }
        }

        private void onFileCreated(boolean initial) {
            for (FileChangesListener listener : listeners()) {
                try {
                    if (initial) {
                        listener.onFileInit(path);
                    } else {
                        listener.onFileCreated(path);
                    }
                } catch (Exception e) {
                    logger.warn("cannot notify file changes listener", e);
                }
            }
        }

        void onFileDeleted() {
            for (FileChangesListener listener : listeners()) {
                try {
                    listener.onFileDeleted(path);
                } catch (Exception e) {
                    logger.warn("cannot notify file changes listener", e);
                }
            }
        }

        private void onFileChanged() {
            for (FileChangesListener listener : listeners()) {
                try {
                    listener.onFileChanged(path);
                } catch (Exception e) {
                    logger.warn("cannot notify file changes listener", e);
                }

            }
        }

        private void onDirectoryCreated(boolean initial) throws IOException {
            for (FileChangesListener listener : listeners()) {
                try {
                    if (initial) {
                        listener.onDirectoryInit(path);
                    } else {
                        listener.onDirectoryCreated(path);
                    }
                } catch (Exception e) {
                    logger.warn("cannot notify file changes listener", e);
                }
            }
            children = listChildren(initial);
        }

        void onDirectoryDeleted() {
            // First delete all children
            for (int child = 0; child < children.length; child++) {
                deleteChild(child);
            }
            for (FileChangesListener listener : listeners()) {
                try {
                    listener.onDirectoryDeleted(path);
                } catch (Exception e) {
                    logger.warn("cannot notify file changes listener", e);
                }
            }
        }

    }

}
