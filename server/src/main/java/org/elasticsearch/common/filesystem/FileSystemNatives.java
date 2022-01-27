/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.filesystem;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;

import java.nio.file.Path;
import java.util.OptionalLong;

/**
 * This class provides utility methods for calling some native methods related to filesystems.
 */
public final class FileSystemNatives {

    private static final Logger logger = LogManager.getLogger(FileSystemNatives.class);

    @FunctionalInterface
    interface Provider {
        OptionalLong allocatedSizeInBytes(Path path);
    }

    private static final Provider NOOP_FILE_SYSTEM_NATIVES_PROVIDER = path -> OptionalLong.empty();
    private static final Provider JNA_PROVIDER = loadJnaProvider();

    private static Provider loadJnaProvider() {
        try {
            // load one of the main JNA classes to see if the classes are available. this does not ensure that all native
            // libraries are available, only the ones necessary by JNA to function
            Class.forName("com.sun.jna.Native");
            if (Constants.WINDOWS) {
                return WindowsFileSystemNatives.getInstance();
            } else if (Constants.LINUX && Constants.JRE_IS_64BIT) {
                return LinuxFileSystemNatives.getInstance();
            }
        } catch (ClassNotFoundException e) {
            logger.warn("JNA not found. FileSystemNatives methods will be disabled.", e);
        } catch (LinkageError e) {
            logger.warn("unable to load JNA native support library, FileSystemNatives methods will be disabled.", e);
        }
        return NOOP_FILE_SYSTEM_NATIVES_PROVIDER;
    }

    private FileSystemNatives() {}

    public static void init() {
        assert JNA_PROVIDER != null;
    }

    /**
     * Returns the number of allocated bytes on disk for a given file.
     *
     * @param path the path to the file
     * @return an {@link OptionalLong} that contains the number of allocated bytes on disk for the file. The optional is empty is the
     * allocated size of the file failed be retrieved using native methods
     */
    public static OptionalLong allocatedSizeInBytes(Path path) {
        return JNA_PROVIDER.allocatedSizeInBytes(path);
    }

}
