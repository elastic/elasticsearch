/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.lucene.store.FilterIndexOutput;

import java.io.IOException;

/**
 * abstract class for verifying what was written.
 * subclasses override {@link #writeByte(byte)} and {@link #writeBytes(byte[], int, int)}
 */
// do NOT optimize this class for performance
public abstract class VerifyingIndexOutput extends FilterIndexOutput {

    /** Sole constructor */
    VerifyingIndexOutput(IndexOutput out) {
        super("VerifyingIndexOutput(out=" + out.toString() + ")", out);
    }

    /**
     * Verifies the checksum and compares the written length with the expected file length. This method should be
     * called after all data has been written to this output.
     */
    public abstract void verify() throws IOException;
}
