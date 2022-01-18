/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.watcher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.util.CollectionUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;

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

    private static final FileObserver[] EMPTY_DIRECTORY = new FileObserver[0];

    private class FileObserver {
        private final Path path;

        private boolean exists;
        private long length;
        private long lastModified;
        private boolean isDirectory;
        private FileObserver[] children;
        private byte[] digest;

        FileObserver(Path path) {
            this.path = path;
        }

        public void checkAndNotify() throws IOException {
            boolean prevExists = exists;
            boolean prevIsDirectory = isDirectory;
            long prevLength = length;
            long prevLastModified = lastModified;
            byte[] prevDigest = digest;

            exists = Files.exists(path);
            // TODO we might use the new NIO2 API to get real notification?
            if (exists) {
                BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
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
            try (var in = Files.newInputStream(path)) {
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
            exists = Files.exists(path);
            if (exists) {
                BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
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

        private FileObserver createChild(Path file, boolean initial) throws IOException {
            FileObserver child = new FileObserver(file);
            child.init(initial);
            return child;
        }

        private Path[] listFiles() throws IOException {
            final Path[] files = FileSystemUtils.files(path);
            Arrays.sort(files);
            return files;
        }

        private FileObserver[] listChildren(boolean initial) throws IOException {
            Path[] files = listFiles();
            if (CollectionUtils.isEmpty(files) == false) {
                FileObserver[] childObservers = new FileObserver[files.length];
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
                FileObserver[] newChildren = new FileObserver[files.length];
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

        private void onFileDeleted() {
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

        private void onDirectoryDeleted() {
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
