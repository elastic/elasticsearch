/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

public interface LogFileStructureFactory {

    /**
     * Given a sample of a log file, decide whether this factory will be able
     * to create an appropriate object to represent its ingestion configs.
     * @param sample A sample from the log file to be ingested.
     * @return <code>true</code> if this factory can create an appropriate log
     *         file structure given the sample; otherwise <code>false</code>.
     */
    boolean canCreateFromSample(String sample);

    /**
     * Create an object representing the structure of a log file.
     * @param sampleFileName The name of the sample log file to be ingested.
     * @param indexName The name of the index to specify in the Logstash direct from file config.
     * @param typeName The name for this type of log file.
     * @param logstashFileTimezone The timezone to use for Logstash direct from file input.  May be <code>null</code>.
     * @param sample A sample from the log file to be ingested.
     * @param charsetName The name of the character set in which the sample was provided.
     * @return A log file structure object suitable for ingesting the supplied sample.
     * @throws Exception if something goes wrong during creation.
     */
    LogFileStructure createFromSample(String sampleFileName, String indexName, String typeName, String logstashFileTimezone, String sample,
                                      String charsetName) throws Exception;
}
