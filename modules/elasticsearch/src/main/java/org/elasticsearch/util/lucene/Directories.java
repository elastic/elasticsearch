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

package org.elasticsearch.util.lucene;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.*;
import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.io.FileSystemUtils;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Collection;

/**
 * A set of utilities for Lucene {@link Directory}.
 *
 * @author kimchy (Shay Banon)
 */
public class Directories {

    /**
     * Deletes all the files from a directory.
     *
     * @param directory The directoy to delete all the files from
     * @throws IOException if an exception occurs during the delete process
     */
    public static void deleteFiles(Directory directory) throws IOException {
        String[] files = directory.listAll();
        for (String file : files) {
            directory.deleteFile(file);
        }
    }

    /**
     * Returns the estimated size of a {@link Directory}.
     */
    public static SizeValue estimateSize(Directory directory) throws IOException {
        long estimatedSize = 0;
        String[] files = directory.listAll();
        for (String file : files) {
            try {
                estimatedSize += directory.fileLength(file);
            } catch (FileNotFoundException e) {
                // ignore, the file is not there no more
            }
        }
        return new SizeValue(estimatedSize);
    }

    /**
     * Lists all the commit point in a directory.
     */
    public static Collection<IndexCommit> listCommits(Directory directory) throws IOException {
        return IndexReader.listCommits(directory);
    }

    /**
     * Computes the checksum of the given file name with the directory.
     */
    public static long checksum(Directory dir, String name) throws IOException {
        return checksum(dir.openInput(name));
    }

    public static void copyFromDirectory(Directory dir, String fileName, File copyTo) throws IOException {
        if (dir instanceof FSDirectory) {
            if (!copyTo.exists()) {
                copyTo.createNewFile();
            }
            FileChannel source = null;
            FileChannel destination = null;
            try {
                source = new FileInputStream(new File(((FSDirectory) dir).getFile(), fileName)).getChannel();
                destination = new FileOutputStream(copyTo).getChannel();
                destination.transferFrom(source, 0, source.size());
            } finally {
                if (source != null) {
                    source.close();
                }
                if (destination != null) {
                    destination.close();
                }
            }
        } else {
            copyFromDirectory(dir.openInput(fileName), new FileOutputStream(copyTo));
        }
        // sync the file
        FileSystemUtils.syncFile(copyTo);
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
            if (os != null) {
                try {
                    os.close();
                } catch (Exception e) {
                    // ignore
                }
            }
            if (ii != null) {
                try {
                    ii.close();
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }

    public static void copyToDirectory(File copyFrom, Directory dir, String fileName) throws IOException {
        if (dir instanceof FSDirectory) {
            File destinationFile = new File(((FSDirectory) dir).getFile(), fileName);
            if (!destinationFile.exists()) {
                destinationFile.createNewFile();
            }
            FileChannel source = null;
            FileChannel destination = null;
            try {
                source = new FileInputStream(copyFrom).getChannel();
                destination = new FileOutputStream(destinationFile).getChannel();
                destination.transferFrom(source, 0, source.size());
            } finally {
                if (source != null) {
                    source.close();
                }
                if (destination != null) {
                    destination.close();
                }
            }
        } else {
            copyToDirectory(new FileInputStream(copyFrom), dir.createOutput(fileName));
        }
        dir.sync(fileName);
    }

    public static void copyToDirectory(InputStream is, IndexOutput io) throws IOException {
        byte[] buffer = new byte[16384];
        int len;
        try {
            while ((len = is.read(buffer)) != -1) {
                io.writeBytes(buffer, len);
            }
        } finally {
            try {
                io.close();
            } catch (Exception e) {
                // ignore
            }
            try {
                is.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    /**
     * Computes the checksum of the content represented by the provided index input.
     *
     * <p>Closes the index input once checksum is computed.
     */
    public static long checksum(IndexInput indexInput) throws IOException {
        final int BUFFER_SIZE = 16384;
        byte[] buf = new byte[BUFFER_SIZE];
        ChecksumIndexInput cii = new ChecksumIndexInput(indexInput);
        long len = cii.length();
        long readCount = 0;
        while (readCount < len) {
            int toRead = readCount + BUFFER_SIZE > len ? (int) (len - readCount) : BUFFER_SIZE;
            cii.readBytes(buf, 0, toRead);
            readCount += toRead;
        }
        cii.close();
        return cii.getChecksum();
    }

    private Directories() {

    }
}
