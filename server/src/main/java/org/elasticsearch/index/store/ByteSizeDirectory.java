/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;

public abstract class ByteSizeDirectory extends FilterDirectory {

    protected static long estimateSizeInBytes(Directory directory) throws IOException {
        long estimatedSize = 0;
        String[] files = directory.listAll();
        for (String file : files) {
            try {
                estimatedSize += directory.fileLength(file);
            } catch (NoSuchFileException | FileNotFoundException | AccessDeniedException e) {
                // ignore, the file is not there no more; on Windows, if one thread concurrently deletes a file while
                // calling Files.size, you can also sometimes hit AccessDeniedException
            }
        }
        return estimatedSize;
    }

    protected ByteSizeDirectory(Directory in) {
        super(in);
    }

    /**
     * @return the size of the directory
     *
     * @throws IOException if an I/O error occurs
     */
    public abstract long estimateSizeInBytes() throws IOException;

    /**
     * @return the size of the total data set of the directory (which can differ from {{@link #estimateSizeInBytes()}})
     *
     * @throws IOException if an I/O error occurs
     */
    public abstract long estimateDataSetSizeInBytes() throws IOException;

}
