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

package org.elasticsearch.common.lucene;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.io.Closeables;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.store.support.ForceSyncDirectory;

import java.io.*;
import java.util.Collection;

import static org.elasticsearch.common.io.FileSystemUtils.*;

/**
 * A set of utilities for Lucene {@link Directory}.
 *
 * @author kimchy (shay.banon)
 */
public class Directories {

    /**
     * Returns the estimated size of a {@link Directory}.
     */
    public static ByteSizeValue estimateSize(Directory directory) throws IOException {
        long estimatedSize = 0;
        String[] files = directory.listAll();
        for (String file : files) {
            try {
                estimatedSize += directory.fileLength(file);
            } catch (FileNotFoundException e) {
                // ignore, the file is not there no more
            }
        }
        return new ByteSizeValue(estimatedSize);
    }

    /**
     * Lists all the commit point in a directory.
     */
    public static Collection<IndexCommit> listCommits(Directory directory) throws IOException {
        return IndexReader.listCommits(directory);
    }

    public static void copyFromDirectory(Directory dir, String fileName, File copyTo, boolean nativeCopy) throws IOException {
        if (nativeCopy && (dir instanceof FSDirectory)) {
            if (!copyTo.exists()) {
                copyTo.createNewFile();
            }
            copyFile(new File(((FSDirectory) dir).getDirectory(), fileName), copyTo);
        } else {
            copyFromDirectory(dir.openInput(fileName), new FileOutputStream(copyTo));
        }
        // sync the file
        syncFile(copyTo);
    }

    public static void copyFromDirectory(Directory dir, String fileName, OutputStream os) throws IOException {
        copyFromDirectory(dir.openInput(fileName), os);
    }

    public static void copyFromDirectory(IndexInput ii, OutputStream os) throws IOException {
        final int BUFFER_SIZE = ii.length() < 16384 ? (int) ii.length() : 16384;
        byte[] buffer = new byte[BUFFER_SIZE];
        try {
            long len = ii.length();
            long readCount = 0;
            while (readCount < len) {
                int toRead = readCount + BUFFER_SIZE > len ? (int) (len - readCount) : BUFFER_SIZE;
                ii.readBytes(buffer, 0, toRead, false);
                readCount += toRead;
                os.write(buffer, 0, toRead);
            }
        } finally {
            Closeables.closeQuietly(os);
            Closeables.closeQuietly(ii);
        }
    }

    public static void copyToDirectory(File copyFrom, Directory dir, String fileName, boolean nativeCopy) throws IOException {
        if (nativeCopy && (dir instanceof FSDirectory)) {
            File destinationFile = new File(((FSDirectory) dir).getDirectory(), fileName);
            if (!destinationFile.exists()) {
                destinationFile.createNewFile();
            }
            copyFile(copyFrom, destinationFile);
        } else {
            FileInputStream is = null;
            IndexOutput output = null;
            try {
                is = new FileInputStream(copyFrom);
                output = dir.createOutput(fileName);
                copyToDirectory(is, output);
            } finally {
                Closeables.closeQuietly(is);
                Closeables.closeQuietly(output);
            }
        }
        sync(dir, fileName);
    }

    public static void copyToDirectory(InputStream is, Directory dir, String fileName) throws IOException {
        IndexOutput output = null;
        try {
            output = dir.createOutput(fileName);
            copyToDirectory(is, output);
        } finally {
            Closeables.closeQuietly(is);
            Closeables.closeQuietly(output);
        }
        sync(dir, fileName);
    }

    public static void sync(Directory dir, String fileName) throws IOException {
        if (dir instanceof ForceSyncDirectory) {
            ((ForceSyncDirectory) dir).forceSync(fileName);
        } else {
            dir.sync(fileName);
        }
    }

    public static void copyToDirectory(InputStream is, IndexOutput io) throws IOException {
        byte[] buffer = new byte[16384];
        int len;
        try {
            while ((len = is.read(buffer)) != -1) {
                io.writeBytes(buffer, len);
            }
        } finally {
            Closeables.closeQuietly(io);
            Closeables.closeQuietly(is);
        }
    }

    private Directories() {

    }
}
