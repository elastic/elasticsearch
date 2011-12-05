/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common.lucene.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.index.store.support.ForceSyncDirectory;

import java.io.IOException;
import java.util.*;

/**
 * A Directory instance that switches files between
 * two other Directory instances.
 *
 * <p>Files with the specified extensions are placed in the
 * primary directory; others are placed in the secondary
 * directory.
 *
 * @author kimchy (shay.banon)
 */
public class SwitchDirectory extends Directory implements ForceSyncDirectory {

    private final Directory secondaryDir;

    private final Directory primaryDir;

    private final ImmutableSet<String> primaryExtensions;

    private boolean doClose;

    public SwitchDirectory(Set<String> primaryExtensions, Directory primaryDir, Directory secondaryDir, boolean doClose) {
        this.primaryExtensions = ImmutableSet.copyOf(primaryExtensions);
        this.primaryDir = primaryDir;
        this.secondaryDir = secondaryDir;
        this.doClose = doClose;
        this.lockFactory = primaryDir.getLockFactory();
    }

    public ImmutableSet<String> primaryExtensions() {
        return primaryExtensions;
    }

    /**
     * Return the primary directory
     */
    public Directory primaryDir() {
        return primaryDir;
    }

    /**
     * Return the secondary directory
     */
    public Directory secondaryDir() {
        return secondaryDir;
    }

    @Override public void close() throws IOException {
        if (doClose) {
            try {
                secondaryDir.close();
            } finally {
                primaryDir.close();
            }
            doClose = false;
        }
    }

    @Override public String[] listAll() throws IOException {
        Set<String> files = new HashSet<String>();
        for (String f : primaryDir.listAll()) {
            files.add(f);
        }
        for (String f : secondaryDir.listAll()) {
            files.add(f);
        }
        return files.toArray(new String[files.size()]);
    }

    /**
     * Utility method to return a file's extension.
     */
    public static String getExtension(String name) {
        int i = name.lastIndexOf('.');
        if (i == -1) {
            return "";
        }
        return name.substring(i + 1, name.length());
    }

    private Directory getDirectory(String name) {
        String ext = getExtension(name);
        if (primaryExtensions.contains(ext)) {
            return primaryDir;
        } else {
            return secondaryDir;
        }
    }

    @Override public boolean fileExists(String name) throws IOException {
        return getDirectory(name).fileExists(name);
    }

    @Override public long fileModified(String name) throws IOException {
        return getDirectory(name).fileModified(name);
    }

    @Override public void touchFile(String name) throws IOException {
        getDirectory(name).touchFile(name);
    }

    @Override public void deleteFile(String name) throws IOException {
        getDirectory(name).deleteFile(name);
    }

    @Override public long fileLength(String name) throws IOException {
        return getDirectory(name).fileLength(name);
    }

    @Override public IndexOutput createOutput(String name) throws IOException {
        return getDirectory(name).createOutput(name);
    }

    @Override public void sync(Collection<String> names) throws IOException {
        List<String> primaryNames = new ArrayList<String>();
        List<String> secondaryNames = new ArrayList<String>();

        for (String name : names)
            if (primaryExtensions.contains(getExtension(name)))
                primaryNames.add(name);
            else
                secondaryNames.add(name);

        primaryDir.sync(primaryNames);
        secondaryDir.sync(secondaryNames);
    }

    @Override public void sync(String name) throws IOException {
        getDirectory(name).sync(name);
    }

    @Override public void forceSync(String name) throws IOException {
        Directory dir = getDirectory(name);
        if (dir instanceof ForceSyncDirectory) {
            ((ForceSyncDirectory) dir).forceSync(name);
        } else {
            dir.sync(name);
        }
    }

    @Override public IndexInput openInput(String name) throws IOException {
        return getDirectory(name).openInput(name);
    }
}
