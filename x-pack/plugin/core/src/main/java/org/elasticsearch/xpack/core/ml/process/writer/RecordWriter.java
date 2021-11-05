/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.process.writer;

import java.io.IOException;
import java.util.List;

/**
 * Interface for classes that write arrays of strings to the
 * Ml data frame analytics processes.
 */
public interface RecordWriter {
    /**
     * Value must match api::CAnomalyDetector::CONTROL_FIELD_NAME in the C++
     * code.
     */
    String CONTROL_FIELD_NAME = ".";

    /**
     * Value must match api::CBaseTokenListDataTyper::PRETOKENISED_TOKEN_FIELD in the C++
     * code.
     */
    String PRETOKENISED_TOKEN_FIELD = "...";

    /**
     * Write each String in the record array
     */
    void writeRecord(String[] record) throws IOException;

    /**
     * Write each String in the record list
     */
    void writeRecord(List<String> record) throws IOException;

    /**
     * Flush the outputIndex stream.
     */
    void flush() throws IOException;

}
