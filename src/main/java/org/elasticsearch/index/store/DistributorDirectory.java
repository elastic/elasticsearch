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

import org.apache.lucene.store.*;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.math.MathUtils;
import org.elasticsearch.index.store.distributor.Distributor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A directory implementation that uses the Elasticsearch {@link Distributor} abstraction to distribute
 * files across multiple data directories.
 */
public final class DistributorDirectory extends BaseDirectory {

    private final Distributor distributor;
    private final HashMap<String, Directory> nameDirMapping = new HashMap<>();

    /**
     * Creates a new DistributorDirectory from multiple directories. Note: The first directory in the given array
     * is used as the primary directory holding the file locks as well as the SEGMENTS_GEN file. All remaining
     * directories are used in a round robin fashion.
     */
    public DistributorDirectory(final Directory... dirs) throws IOException {
        this(new Distributor() {
            final AtomicInteger count = new AtomicInteger();

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
                return dirs[MathUtils.mod(count.incrementAndGet(), dirs.length)];
            }
        });
    }

    /**
     * Creates a new DistributorDirectory form the given Distributor.
     */
    public DistributorDirectory(Distributor distributor) throws IOException {
        this.distributor = distributor;
        for (Directory dir : distributor.all()) {
            for (String file : dir.listAll()) {
                nameDirMapping.put(file, dir);
            }
        }
        lockFactory = new DistributorLockFactoryWrapper(distributor.primary());
    }

    @Override
    public synchronized final String[] listAll() throws IOException {
        return nameDirMapping.keySet().toArray(new String[nameDirMapping.size()]);
    }

    @Override
    public synchronized boolean fileExists(String name) throws IOException {
        try {
            return getDirectory(name).fileExists(name);
        } catch (FileNotFoundException ex) {
            return false;
        }
    }

    @Override
    public synchronized void deleteFile(String name) throws IOException {
        getDirectory(name, true).deleteFile(name);
        Directory remove = nameDirMapping.remove(name);
        assert remove != null : "Tried to delete file " + name + " but couldn't";
    }

    @Override
    public synchronized long fileLength(String name) throws IOException {
        return getDirectory(name).fileLength(name);
    }

    @Override
    public synchronized IndexOutput createOutput(String name, IOContext context) throws IOException {
        return getDirectory(name, false).createOutput(name, context);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        // no need to sync this operation it could be long running too
        for (Directory dir : distributor.all()) {
            dir.sync(names);
        }
    }

    @Override
    public synchronized IndexInput openInput(String name, IOContext context) throws IOException {
        return getDirectory(name).openInput(name, context);
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            assert assertConsistency();
        } finally {
            IOUtils.close(distributor.all());
        }

    }

    /**
     * Returns the directory that has previously been associated with this file name.
     *
     * @throws IOException if the name has not yet been associated with any directory ie. fi the file does not exists
     */
    Directory getDirectory(String name) throws IOException { // pkg private for testing
        return getDirectory(name, true);
    }

    /**
     * Returns the directory that has previously been associated with this file name or associates the name with a directory
     * if failIfNotAssociated is set to false.
     */
    private Directory getDirectory(String name, boolean failIfNotAssociated) throws IOException {
        final Directory directory = nameDirMapping.get(name);
        if (directory == null) {
            if (failIfNotAssociated) {
                throw new FileNotFoundException("No such file [" + name + "]");
            }
            // Pick a directory and associate this new file with it:
            final Directory dir = distributor.any();
            assert nameDirMapping.containsKey(name) == false;
            nameDirMapping.put(name, dir);
            return dir;
        }
            
        return directory;
    }

    @Override
    public synchronized void setLockFactory(LockFactory lockFactory) throws IOException {
        distributor.primary().setLockFactory(lockFactory);
        super.setLockFactory(new DistributorLockFactoryWrapper(distributor.primary()));
    }

    @Override
    public synchronized String getLockID() {
        return distributor.primary().getLockID();
    }

    @Override
    public synchronized String toString() {
        return distributor.toString();
    }

    /**
     * Renames the given source file to the given target file unless the target already exists.
     *
     * @param directoryService the DirectoryService to use.
     * @param source the source file name.
     * @param dest the target file name
     * @throws IOException if the target file already exists.
     */
    public synchronized void renameFile(DirectoryService directoryService, String source, String dest) throws IOException {
        final Directory directory = getDirectory(source);
        final Directory targetDir = nameDirMapping.get(dest);
        if (targetDir != null && targetDir != directory) {
            throw new IOException("Can't rename file from " + source
                    + " to: " + dest + ": target file already exists in a different directory");
        }
        directoryService.renameFile(directory, source, dest);
        nameDirMapping.remove(source);
        nameDirMapping.put(dest, directory);
    }

    Distributor getDistributor() {
        return distributor;
    }

    /**
     * Basic checks to ensure the internal mapping is consistent - should only be used in assertions
     */
    private synchronized boolean assertConsistency() throws IOException {
        boolean consistent = true;
        try {
            StringBuilder builder = new StringBuilder();
            Directory[] all = distributor.all();
            for (Directory d : all) {
                for (String file : d.listAll()) {
                    final Directory directory = nameDirMapping.get(file);
                    if (directory == null) {
                        consistent = false;
                        builder.append("File ").append(file)
                                .append(" was not mapped to a directory but exists in one of the distributors directories")
                                .append(System.lineSeparator());
                    } else if (directory != d) {
                        consistent = false;
                        builder.append("File ").append(file).append(" was  mapped to a directory ").append(directory)
                                .append(" but exists in another distributor directory").append(d)
                                .append(System.lineSeparator());
                    }

                }
            }
            assert consistent : builder.toString();
        } catch (NoSuchDirectoryException ex) {
            // that's fine - we can't check the directory since we might have already been wiped by a shutdown or
            // a test cleanup ie the directory is not there anymore.
        }
        return consistent; // return boolean so it can be easily be used in asserts
    }

    /**
     * This inner class is a simple wrapper around the original
     * lock factory to track files written / created through the
     * lock factory. For instance {@link NativeFSLockFactory} creates real
     * files that we should expose for consistency reasons.
     */
    private class DistributorLockFactoryWrapper extends LockFactory {
        private final Directory dir;
        private final LockFactory delegate;
        private final boolean writesFiles;

        public DistributorLockFactoryWrapper(Directory dir) {
            this.dir = dir;
            final FSDirectory leaf = DirectoryUtils.getLeaf(dir, FSDirectory.class);
            if (leaf != null) {
               writesFiles = leaf.getLockFactory() instanceof FSLockFactory;
            } else {
                writesFiles = false;
            }
            this.delegate = dir.getLockFactory();
        }

        @Override
        public void setLockPrefix(String lockPrefix) {
            delegate.setLockPrefix(lockPrefix);
        }

        @Override
        public String getLockPrefix() {
            return delegate.getLockPrefix();
        }

        @Override
        public Lock makeLock(String lockName) {
            return new DistributorLock(delegate.makeLock(lockName), lockName);
        }

        @Override
        public void clearLock(String lockName) throws IOException {
            delegate.clearLock(lockName);
        }

        @Override
        public String toString() {
            return "DistributorLockFactoryWrapper(" + delegate.toString() + ")";
        }

        private class DistributorLock extends Lock {
            private final Lock delegateLock;
            private final String name;

            DistributorLock(Lock delegate, String name) {
                this.delegateLock = delegate;
                this.name = name;
            }

            @Override
            public boolean obtain() throws IOException {
                if (delegateLock.obtain()) {
                    if (writesFiles) {
                        synchronized (DistributorDirectory.this) {
                            assert (nameDirMapping.containsKey(name) == false || nameDirMapping.get(name) == dir);
                            if (nameDirMapping.get(name) == null) {
                                nameDirMapping.put(name, dir);
                            }
                        }
                    }
                    return true;
                } else {
                    return false;
                }
            }

            @Override
            public void close() throws IOException { delegateLock.close(); }

            @Override
            public boolean isLocked() throws IOException {
                return delegateLock.isLocked();
            }
        }
    }
}
