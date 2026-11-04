/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import java.io.IOException;

/**
 * A Parquet file that lives somewhere, as the reader sees it. It hides the storage system, so that the format works the same for a
 * repository today and for another provider later, and so that tests can read from memory.
 */
public interface ParquetObject {

    /** The storage reference of the file, which is stored in the segment. It never holds credentials. */
    String location();

    /** The length of the file in bytes. */
    long length() throws IOException;

    /**
     * Reads exactly {@code length} bytes starting at {@code position}.
     *
     * @throws java.io.EOFException if the file ends before that
     */
    byte[] readRange(long position, int length) throws IOException;
}
