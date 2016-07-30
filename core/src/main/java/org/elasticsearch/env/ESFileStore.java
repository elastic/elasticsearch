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
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.util.Arrays;
import java.util.List;

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
    int majorDeviceNumber;
    int minorDeviceNumber;
    
    @SuppressForbidden(reason = "tries to determine if disk is spinning")
    // TODO: move PathUtils to be package-private here instead of 
    // public+forbidden api!
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
            try {
                final List<String> lines = Files.readAllLines(PathUtils.get("/proc/self/mountinfo"));
                for (final String line : lines) {
                    final String[] fields = line.trim().split("\\s+");
                    final String mountPoint = fields[4];
                    if (mountPoint.equals(getMountPointLinux(in))) {
                        final String[] deviceNumbers = fields[2].split(":");
                        majorDeviceNumber = Integer.parseInt(deviceNumbers[0]);
                        minorDeviceNumber = Integer.parseInt(deviceNumbers[1]);
                    }
                }
            } catch (Exception e) {
                majorDeviceNumber = -1;
                minorDeviceNumber = -1;
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
    
    /** 
     * Files.getFileStore(Path) useless here!  Don't complain, just try it yourself. 
     */
    @SuppressForbidden(reason = "works around the bugs")
    static FileStore getMatchingFileStore(Path path, FileStore fileStores[]) throws IOException {       
        if (Constants.WINDOWS) {
            return getFileStoreWindows(path, fileStores);
        }
        
        final FileStore store;
        try {
            store = Files.getFileStore(path);
        } catch (IOException unexpected) {
            // give a better error message if a filestore cannot be retrieved from inside a FreeBSD jail.
            if (Constants.FREE_BSD) {
                throw new IOException("Unable to retrieve mount point data for " + path +
                                      ". If you are running within a jail, set enforce_statfs=1. See jail(8)", unexpected);
            } else {
                throw unexpected;
            }
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
    
    /** 
     * remove this code and just use getFileStore for windows on java 9
     * works around https://bugs.openjdk.java.net/browse/JDK-8034057
     */
    @SuppressForbidden(reason = "works around https://bugs.openjdk.java.net/browse/JDK-8034057")
    static FileStore getFileStoreWindows(Path path, FileStore fileStores[]) throws IOException {
        assert Constants.WINDOWS;
        
        try {
            return Files.getFileStore(path);
        } catch (FileSystemException possibleBug) {
            final char driveLetter;
            // look for a drive letter to see if its the SUBST bug,
            // it might be some other type of path, like a windows share
            // if something goes wrong, we just deliver the original exception
            try {
                String root = path.toRealPath().getRoot().toString();
                if (root.length() < 2) {
                    throw new RuntimeException("root isn't a drive letter: " + root);
                }
                driveLetter = Character.toLowerCase(root.charAt(0));
                if (Character.isAlphabetic(driveLetter) == false || root.charAt(1) != ':') {
                    throw new RuntimeException("root isn't a drive letter: " + root);
                }
            } catch (Exception checkFailed) {
                // something went wrong, 
                possibleBug.addSuppressed(checkFailed);
                throw possibleBug;
            }
            
            // we have a drive letter: the hack begins!!!!!!!!
            try {
                // we have no choice but to parse toString of all stores and find the matching drive letter
                for (FileStore store : fileStores) {
                    String toString = store.toString();
                    int length = toString.length();
                    if (length > 3 && toString.endsWith(":)") && toString.charAt(length - 4) == '(') {
                        if (Character.toLowerCase(toString.charAt(length - 3)) == driveLetter) {
                            return store;
                        }
                    }
                }
                throw new RuntimeException("no filestores matched");
            } catch (Exception weTried) {
                IOException newException = new IOException("Unable to retrieve filestore for '" + path + "', tried matching against " + Arrays.toString(fileStores), weTried);
                newException.addSuppressed(possibleBug);
                throw newException;
            }
        }
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
        long result = in.getTotalSpace();
        if (result < 0) {
            // see https://bugs.openjdk.java.net/browse/JDK-8162520:
            result = Long.MAX_VALUE;
        }
        return result;
    }

    @Override
    public long getUsableSpace() throws IOException {
        long result = in.getUsableSpace();
        if (result < 0) {
            // see https://bugs.openjdk.java.net/browse/JDK-8162520:
            result = Long.MAX_VALUE;
        }
        return result;
    }

    @Override
    public long getUnallocatedSpace() throws IOException {
        long result = in.getUnallocatedSpace();
        if (result < 0) {
            // see https://bugs.openjdk.java.net/browse/JDK-8162520:
            result = Long.MAX_VALUE;
        }
        return result;
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
        switch(attribute) {
            // for the device
            case "lucene:spins": return spins;
            // for the partition
            case "lucene:major_device_number": return majorDeviceNumber;
            case "lucene:minor_device_number": return minorDeviceNumber;
            default: return in.getAttribute(attribute);
        }
    }

    @Override
    public String toString() {
        return in.toString();
    }
}
