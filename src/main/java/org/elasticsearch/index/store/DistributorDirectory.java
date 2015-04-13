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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A directory implementation that uses the Elasticsearch {@link Distributor} abstraction to distribute
 * files across multiple data directories.
 */
public final class DistributorDirectory extends Directory {

    private final Distributor distributor;
    private final HashMap<String, Directory> nameDirMapping = new HashMap<>();
    private boolean closed = false;

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
    }

    @Override
    public synchronized final String[] listAll() throws IOException {
        return nameDirMapping.keySet().toArray(new String[nameDirMapping.size()]);
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
        final Map<Directory, Collection<String>> perDirectory = new IdentityHashMap<>();
        for (String name : names) {
            final Directory dir = getDirectory(name);
            Collection<String> dirNames = perDirectory.get(dir);
            if (dirNames == null) {
                dirNames = new ArrayList<>();
                perDirectory.put(dir, dirNames);
            }
            dirNames.add(name);
        }
        for (Map.Entry<Directory, Collection<String>> entry : perDirectory.entrySet()) {
            final Directory dir = entry.getKey();
            final Collection<String> dirNames = entry.getValue();
            dir.sync(dirNames);
        }
    }

    @Override
    public synchronized void renameFile(String source, String dest) throws IOException {
        final Directory directory = getDirectory(source);
        final Directory targetDir = nameDirMapping.get(dest);
        if (targetDir != null && targetDir != directory) {
            throw new IOException("Can't rename file from " + source
                    + " to: " + dest + ": target file already exists in a different directory");
        }
        directory.renameFile(source, dest);
        nameDirMapping.remove(source);
        nameDirMapping.put(dest, directory);
    }

    @Override
    public synchronized IndexInput openInput(String name, IOContext context) throws IOException {
        return getDirectory(name).openInput(name, context);
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        try {
            assert assertConsistency();
        } finally {
            closed = true;
            IOUtils.close(distributor.all());
        }
    }

    /**
     * Returns the directory that has previously been associated with this file name.
     *
     * @throws IOException if the name has not yet been associated with any directory ie. fi the file does not exists
     */
    synchronized Directory getDirectory(String name) throws IOException { // pkg private for testing
        return getDirectory(name, true);
    }

    /**
     * Returns the directory that has previously been associated with this file name or associates the name with a directory
     * if failIfNotAssociated is set to false.
     */
    private synchronized Directory getDirectory(String name, boolean failIfNotAssociated) throws IOException {
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
    public synchronized String toString() {
        return distributor.toString();
    }

    Distributor getDistributor() {
        return distributor;
    }

    /**
     * Basic checks to ensure the internal mapping is consistent - should only be used in assertions
     */
    private synchronized boolean assertConsistency() throws IOException {
        boolean consistent = true;
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
                    builder.append("File ").append(file).append(" was mapped to a directory ").append(directory)
                            .append(" but exists in another distributor directory ").append(d)
                            .append(System.lineSeparator());
                }

            }
        }
        assert consistent : builder.toString();
        return consistent; // return boolean so it can be easily be used in asserts
    }

    @Override
    public Lock makeLock(final String lockName) {
        final Directory primary = distributor.primary();
        final Lock delegateLock = primary.makeLock(lockName);
        if (DirectoryUtils.getLeaf(primary, FSDirectory.class) != null) {
            // Wrap the delegate's lock just so we can monitor when it actually wrote a lock file.  We assume that an FSDirectory writes its
            // locks as actual files (we don't support NoLockFactory):
            return new Lock() {
                @Override
                public boolean obtain() throws IOException {
                    if (delegateLock.obtain()) {
                        synchronized(DistributorDirectory.this) {
                            assert nameDirMapping.containsKey(lockName) == false || nameDirMapping.get(lockName) == primary;
                            if (nameDirMapping.get(lockName) == null) {
                                nameDirMapping.put(lockName, primary);
                            }
                        }
                        return true;
                    } else {
                        return false;
                    }
                }

                @Override
                public void close() throws IOException {
                    delegateLock.close();
                }

                @Override
                public boolean isLocked() throws IOException {
                    return delegateLock.isLocked();
                }
            };
        } else {
            return delegateLock;
        }
    }
}
