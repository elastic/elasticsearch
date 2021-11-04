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

    interface Provider {
        OptionalLong allocatedSizeInBytes(Path path);
    }

    private static final class NoopFileSystemNativesProvider implements Provider {
        @Override
        public OptionalLong allocatedSizeInBytes(Path path) {
            return OptionalLong.empty();
        }
    }

    private static final Provider JNA_PROVIDER;
    static {
        Provider provider = new NoopFileSystemNativesProvider();
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
        } finally {
            JNA_PROVIDER = provider;
        }
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
