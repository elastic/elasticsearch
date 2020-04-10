/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.filestructurefinder;

import org.elasticsearch.xpack.core.ml.filestructurefinder.FileStructure;

import java.util.List;

public interface FileStructureFinderFactory {

    /**
     * Can this factory create a {@link FileStructureFinder} that can find the supplied format?
     * @param format The format to query, or <code>null</code>.
     * @return <code>true</code> if {@code format} is <code>null</code> or the factory
     *         can produce a {@link FileStructureFinder} that can find {@code format}.
     */
    boolean canFindFormat(FileStructure.Format format);

    /**
     * Given a sample of a file, decide whether this factory will be able
     * to create an appropriate object to represent its ingestion configs.
     * @param explanation List of reasons for making decisions.  May contain items when passed and new reasons
     *                    can be appended by this method.
     * @param sample A sample from the file to be ingested.
     * @return <code>true</code> if this factory can create an appropriate
     *         file structure given the sample; otherwise <code>false</code>.
     */
    boolean canCreateFromSample(List<String> explanation, String sample);

    /**
     * Create an object representing the structure of a file.
     * @param explanation List of reasons for making decisions.  May contain items when passed and new reasons
     *                    can be appended by this method.
     * @param sample A sample from the file to be ingested.
     * @param charsetName The name of the character set in which the sample was provided.
     * @param hasByteOrderMarker Did the sample have a byte order marker?  <code>null</code> means "not relevant".
     * @param lineMergeSizeLimit Maximum number of characters permitted when lines are merged to create messages.
     * @param overrides Stores structure decisions that have been made by the end user, and should
     *                  take precedence over anything the {@link FileStructureFinder} may decide.
     * @param timeoutChecker Will abort the operation if its timeout is exceeded.
     * @return A {@link FileStructureFinder} object suitable for determining the structure of the supplied sample.
     * @throws Exception if something goes wrong during creation.
     */
    FileStructureFinder createFromSample(List<String> explanation, String sample, String charsetName, Boolean hasByteOrderMarker,
                                         int lineMergeSizeLimit, FileStructureOverrides overrides,
                                         TimeoutChecker timeoutChecker) throws Exception;
}
