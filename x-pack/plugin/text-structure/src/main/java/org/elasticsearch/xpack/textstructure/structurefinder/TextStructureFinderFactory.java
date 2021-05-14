/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;

import java.util.List;

public interface TextStructureFinderFactory {

    /**
     * Can this factory create a {@link TextStructureFinder} that can find the supplied format?
     * @param format The format to query, or <code>null</code>.
     * @return <code>true</code> if {@code format} is <code>null</code> or the factory
     *         can produce a {@link TextStructureFinder} that can find {@code format}.
     */
    boolean canFindFormat(TextStructure.Format format);

    /**
     * Given some sample text, decide whether this factory will be able
     * to create an appropriate object to represent its ingestion configs.
     * @param explanation List of reasons for making decisions.  May contain items when passed and new reasons
     *                    can be appended by this method.
     * @param sample A sample from the text to be ingested.
     * @param allowedFractionOfBadLines How many lines of the passed sample are allowed to be considered "bad".
     *                                  Provided as a fraction from interval [0, 1]
     * @return <code>true</code> if this factory can create an appropriate
     *         text structure given the sample; otherwise <code>false</code>.
     */
    boolean canCreateFromSample(List<String> explanation, String sample, double allowedFractionOfBadLines);

    /**
     * Create an object representing the structure of some text.
     * @param explanation List of reasons for making decisions.  May contain items when passed and new reasons
     *                    can be appended by this method.
     * @param sample A sample from the text to be ingested.
     * @param charsetName The name of the character set in which the sample was provided.
     * @param hasByteOrderMarker Did the sample have a byte order marker?  <code>null</code> means "not relevant".
     * @param lineMergeSizeLimit Maximum number of characters permitted when lines are merged to create messages.
     * @param overrides Stores structure decisions that have been made by the end user, and should
     *                  take precedence over anything the {@link TextStructureFinder} may decide.
     * @param timeoutChecker Will abort the operation if its timeout is exceeded.
     * @return A {@link TextStructureFinder} object suitable for determining the structure of the supplied sample.
     * @throws Exception if something goes wrong during creation.
     */
    TextStructureFinder createFromSample(
        List<String> explanation,
        String sample,
        String charsetName,
        Boolean hasByteOrderMarker,
        int lineMergeSizeLimit,
        TextStructureOverrides overrides,
        TimeoutChecker timeoutChecker
    ) throws Exception;
}
