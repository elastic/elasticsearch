/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.logstructurefinder;

import java.util.List;

public interface LogStructureFinderFactory {

    /**
     * Given a sample of a log file, decide whether this factory will be able
     * to create an appropriate object to represent its ingestion configs.
     * @param explanation List of reasons for making decisions.  May contain items when passed and new reasons
     *                    can be appended by this method.
     * @param sample A sample from the log file to be ingested.
     * @return <code>true</code> if this factory can create an appropriate log
     *         file structure given the sample; otherwise <code>false</code>.
     */
    boolean canCreateFromSample(List<String> explanation, String sample);

    /**
     * Create an object representing the structure of a log file.
     * @param explanation List of reasons for making decisions.  May contain items when passed and new reasons
     *                    can be appended by this method.
     * @param sample A sample from the log file to be ingested.
     * @param charsetName The name of the character set in which the sample was provided.
     * @param hasByteOrderMarker Did the sample have a byte order marker?  <code>null</code> means "not relevant".
     * @return A log file structure object suitable for ingesting the supplied sample.
     * @throws Exception if something goes wrong during creation.
     */
    LogStructureFinder createFromSample(List<String> explanation, String sample, String charsetName, Boolean hasByteOrderMarker)
        throws Exception;
}
