/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import java.io.IOException;

/**
 * Interface for classes that read the various styles of JSON inputIndex.
 */
interface JsonRecordReader {
    /**
     * Read some JSON and write to the record array.
     *
     * @param record    Read fields are written to this array. This array is first filled with empty
     *                  strings and will never contain a <code>null</code>
     * @param gotFields boolean array each element is true if that field
     *                  was read
     * @return The number of fields in the JSON doc or -1 if nothing was read
     * because the end of the stream was reached
     */
    long read(String[] record, boolean[] gotFields) throws IOException;
}
