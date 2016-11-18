/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.process.autodetect.writer;

import java.io.IOException;
import java.io.InputStream;

import org.elasticsearch.xpack.prelert.job.DataCounts;

/**
 * A writer for transforming and piping data from an
 * inputstream to outputstream as the process expects.
 */
public interface DataToProcessWriter {
    /**
     * Reads the inputIndex, transform to length encoded values and pipe
     * to the OutputStream.
     * If any of the fields in <code>analysisFields</code> or the
     * <code>DataDescription</code>s timeField is missing from the CSV header
     * a <code>MissingFieldException</code> is thrown
     *
     * @return Counts of the records processed, bytes read etc
     */
    DataCounts write(InputStream inputStream) throws IOException;

    /**
     * Flush the outputstream
     */
    void flush() throws IOException;
}
