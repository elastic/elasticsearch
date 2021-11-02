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
import org.elasticsearch.core.Nullable;

import java.nio.file.Path;

/**
 * This class provides utility methods for calling some native methods related to filesystems.
 */
public final class FileSystemNatives {

    private static final Logger logger = LogManager.getLogger(FileSystemNatives.class);

    interface Provider {
        @Nullable
        Long allocatedSizeInBytes(Path path);
    }

    private static final Provider JNA_PROVIDER;
    static {
        Provider provider = null;
        try {
            // load one of the main JNA classes to see if the classes are available. this does not ensure that all native
            // libraries are available, only the ones necessary by JNA to function
            Class.forName("com.sun.jna.Native");
            if (Constants.WINDOWS) {
                provider = WindowsFileSystemNatives.getInstance();
            }
        } catch (ClassNotFoundException e) {
            logger.warn("JNA not found. FileSystems native methods will be disabled.", e);
        } catch (UnsatisfiedLinkError e) {
            logger.warn("unable to load JNA native support library, FileSystems native methods will be disabled.", e);
        }
        JNA_PROVIDER = provider;
    }

    private FileSystemNatives() {}

    public static void init() {
        assert JNA_PROVIDER != null || Constants.WINDOWS == false;
    }

    /**
     * Returns the number of allocated bytes on disk for a given file.
     *
     * @param path the path to the file
     * @return the number of allocated bytes on disk for the file or {@code null} if the allocated size could not be returned
     */
    @Nullable
    public static Long allocatedSizeInBytes(Path path) {
        if (JNA_PROVIDER != null) {
            return JNA_PROVIDER.allocatedSizeInBytes(path);
        }
        return null;
    }

}
