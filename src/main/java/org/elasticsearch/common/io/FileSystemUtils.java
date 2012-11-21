/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.common.io;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class FileSystemUtils {

    private static ESLogger logger = ESLoggerFactory.getLogger(FileSystemUtils.class.getName());

    private static final long mkdirsStallTimeout = TimeValue.timeValueMinutes(5).millis();
    private static final Object mkdirsMutex = new Object();
    private static volatile Thread mkdirsThread;
    private static volatile long mkdirsStartTime;

    public static boolean mkdirs(File dir) {
        synchronized (mkdirsMutex) {
            try {
                mkdirsThread = Thread.currentThread();
                mkdirsStartTime = System.currentTimeMillis();
                return dir.mkdirs();
            } finally {
                mkdirsThread = null;
            }
        }
    }

    public static void checkMkdirsStall(long currentTime) {
        Thread mkdirsThread1 = mkdirsThread;
        long stallTime = currentTime - mkdirsStartTime;
        if (mkdirsThread1 != null && (stallTime > mkdirsStallTimeout)) {
            logger.error("mkdirs stalled for {} on {}, trying to interrupt", new TimeValue(stallTime), mkdirsThread1.getName());
            mkdirsThread1.interrupt(); // try and interrupt it...
        }
    }

    public static int maxOpenFiles(File testDir) {
        boolean dirCreated = false;
        if (!testDir.exists()) {
            dirCreated = true;
            testDir.mkdirs();
        }
        List<RandomAccessFile> files = new ArrayList<RandomAccessFile>();
        try {
            while (true) {
                files.add(new RandomAccessFile(new File(testDir, "tmp" + files.size()), "rw"));
            }
        } catch (IOException ioe) {
            int i = 0;
            for (RandomAccessFile raf : files) {
                try {
                    raf.close();
                } catch (IOException e) {
                    // ignore
                }
                new File(testDir, "tmp" + i++).delete();
            }
            if (dirCreated) {
                deleteRecursively(testDir);
            }
        }
        return files.size();
    }


    public static boolean hasExtensions(File root, String... extensions) {
        if (root != null && root.exists()) {
            if (root.isDirectory()) {
                File[] children = root.listFiles();
                if (children != null) {
                    for (File child : children) {
                        if (child.isDirectory()) {
                            boolean has = hasExtensions(child, extensions);
                            if (has) {
                                return true;
                            }
                        } else {
                            for (String extension : extensions) {
                                if (child.getName().endsWith(extension)) {
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    /**
     * Returns true if at least one of the files exists.
     */
    public static boolean exists(File... files) {
        for (File file : files) {
            if (file.exists()) {
                return true;
            }
        }
        return false;
    }

    public static boolean deleteRecursively(File[] roots) {
        boolean deleted = true;
        for (File root : roots) {
            deleted &= deleteRecursively(root);
        }
        return deleted;
    }

    public static boolean deleteRecursively(File root) {
        return deleteRecursively(root, true);
    }

    private static boolean innerDeleteRecursively(File root) {
        return deleteRecursively(root, true);
    }

    /**
     * Delete the supplied {@link java.io.File} - for directories,
     * recursively delete any nested directories or files as well.
     *
     * @param root       the root <code>File</code> to delete
     * @param deleteRoot whether or not to delete the root itself or just the content of the root.
     * @return <code>true</code> if the <code>File</code> was deleted,
     *         otherwise <code>false</code>
     */
    public static boolean deleteRecursively(File root, boolean deleteRoot) {
        if (root != null && root.exists()) {
            if (root.isDirectory()) {
                File[] children = root.listFiles();
                if (children != null) {
                    for (File aChildren : children) {
                        innerDeleteRecursively(aChildren);
                    }
                }
            }

            if (deleteRoot) {
                return root.delete();
            } else {
                return true;
            }
        }
        return false;
    }

    public static void syncFile(File fileToSync) throws IOException {
        boolean success = false;
        int retryCount = 0;
        IOException exc = null;
        while (!success && retryCount < 5) {
            retryCount++;
            RandomAccessFile file = null;
            try {
                try {
                    file = new RandomAccessFile(fileToSync, "rw");
                    file.getFD().sync();
                    success = true;
                } finally {
                    if (file != null)
                        file.close();
                }
            } catch (IOException ioe) {
                if (exc == null)
                    exc = ioe;
                try {
                    // Pause 5 msec
                    Thread.sleep(5);
                } catch (InterruptedException ie) {
                    throw new InterruptedIOException(ie.getMessage());
                }
            }
        }
    }

    public static void copyFile(File sourceFile, File destinationFile) throws IOException {
        FileInputStream sourceIs = null;
        FileChannel source = null;
        FileOutputStream destinationOs = null;
        FileChannel destination = null;
        try {
            sourceIs = new FileInputStream(sourceFile);
            source = sourceIs.getChannel();
            destinationOs = new FileOutputStream(destinationFile);
            destination = destinationOs.getChannel();
            destination.transferFrom(source, 0, source.size());
        } finally {
            if (source != null) {
                source.close();
            }
            if (sourceIs != null) {
                sourceIs.close();
            }
            if (destination != null) {
                destination.close();
            }
            if (destinationOs != null) {
                destinationOs.close();
            }
        }
    }

    private FileSystemUtils() {

    }
}
