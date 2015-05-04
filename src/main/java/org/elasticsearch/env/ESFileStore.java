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

package org.elasticsearch.env;

import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.io.PathUtils;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;

/** 
 * Implementation of FileStore that supports
 * additional features, such as SSD detection and better
 * filesystem information for the root filesystem.
 * @see Environment#getFileStore(Path)
 */
class ESFileStore extends FileStore {
    /** Underlying filestore */
    final FileStore in;
    /** Cached result of Lucene's {@code IOUtils.spins} on path. */
    final Boolean spins;
    
    ESFileStore(FileStore in) {
        this.in = in;
        Boolean spins;
        // Lucene's IOUtils.spins only works on Linux today:
        if (Constants.LINUX) {
            try {
                spins = IOUtils.spins(PathUtils.get(getMountPointLinux(in)));
            } catch (Exception e) {
                spins = null;
            }
        } else {
            spins = null;
        }
        this.spins = spins;
    }
    
    // these are hacks that are not guaranteed
    private static String getMountPointLinux(FileStore store) {
        String desc = store.toString();
        int index = desc.lastIndexOf(" (");
        if (index != -1) {
            return desc.substring(0, index);
        } else {
            return desc;
        }
    }
    
    /** Files.getFileStore(Path) useless here!  Don't complain, just try it yourself. */
    static FileStore getMatchingFileStore(Path path, FileStore fileStores[]) throws IOException {
        FileStore store = Files.getFileStore(path);
        
        if (Constants.WINDOWS) {
            return store; // be defensive, don't even try to do anything fancy.
        }

        try {
            String mount = getMountPointLinux(store);
            FileStore sameMountPoint = null;
            for (FileStore fs : fileStores) {
                if (mount.equals(getMountPointLinux(fs))) {
                    if (sameMountPoint == null) {
                        sameMountPoint = fs;
                    } else {
                        // more than one filesystem has the same mount point; something is wrong!
                        // fall back to crappy one we got from Files.getFileStore
                        return store;
                    }
                }
            }

            if (sameMountPoint != null) {
                // ok, we found only one, use it:
                return sameMountPoint;
            } else {
                // fall back to crappy one we got from Files.getFileStore
                return store;    
            }
        } catch (Exception e) {
            // ignore
        }

        // fall back to crappy one we got from Files.getFileStore
        return store;    
    }

    @Override
    public String name() {
        return in.name();
    }

    @Override
    public String type() {
        return in.type();
    }

    @Override
    public boolean isReadOnly() {
        return in.isReadOnly();
    }

    @Override
    public long getTotalSpace() throws IOException {
        return in.getTotalSpace();
    }

    @Override
    public long getUsableSpace() throws IOException {
        return in.getUsableSpace();
    }

    @Override
    public long getUnallocatedSpace() throws IOException {
        return in.getUnallocatedSpace();
    }

    @Override
    public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {
        return in.supportsFileAttributeView(type);
    }

    @Override
    public boolean supportsFileAttributeView(String name) {
        if ("lucene".equals(name)) {
            return true;
        } else {
            return in.supportsFileAttributeView(name);
        }
    }

    @Override
    public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {
        return in.getFileStoreAttributeView(type);
    }

    @Override
    public Object getAttribute(String attribute) throws IOException {
        if ("lucene:spins".equals(attribute)) {
            return spins;
        } else {
            return in.getAttribute(attribute);
        }
    }

    @Override
    public String toString() {
        return in.toString();
    }
}
