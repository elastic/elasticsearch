/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.process.autodetect.output.parsing;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.xpack.prelert.job.persistence.JobResultsPersister;

import java.io.IOException;
import java.io.InputStream;

/**
 * A runnable class that reads the autodetect persisted state
 * and writes the results via the {@linkplain JobResultsPersister}
 * passed in the constructor.
 */
public class StateReader implements Runnable {

    private static final int READ_BUF_SIZE = 8192;

    private final InputStream stream;
    private final Logger logger;
    private final JobResultsPersister persister;

    public StateReader(JobResultsPersister persister, InputStream stream, Logger logger) {
        this.stream = stream;
        this.logger = logger;
        this.persister = persister;
    }

    @Override
    public void run() {
        try {
            BytesReference bytesRef = null;
            byte[] readBuf = new byte[READ_BUF_SIZE];
            for (int bytesRead = stream.read(readBuf); bytesRead != -1; bytesRead = stream.read(readBuf)) {
                if (bytesRef == null) {
                    bytesRef = new BytesArray(readBuf, 0, bytesRead);
                } else {
                    bytesRef = new CompositeBytesReference(bytesRef, new BytesArray(readBuf, 0, bytesRead));
                }
                bytesRef = splitAndPersist(bytesRef);
                readBuf = new byte[READ_BUF_SIZE];
            }
        } catch (IOException e) {
            logger.info("Error reading autodetect state output", e);
        }

        logger.info("State output finished");
    }

    /**
     * Splits bulk data streamed from the C++ process on '\0' characters.  The
     * data is expected to be a series of Elasticsearch bulk requests in UTF-8 JSON
     * (as would be uploaded to the public REST API) separated by zero bytes ('\0').
     */
    private BytesReference splitAndPersist(BytesReference bytesRef) {
        int from = 0;
        while (true) {
            int nextZeroByte = findNextZeroByte(bytesRef, from);
            if (nextZeroByte == -1) {
                // No more zero bytes in this block
                break;
            }
            persister.persistBulkState(bytesRef.slice(from, nextZeroByte - from));
            from = nextZeroByte + 1;
        }
        return bytesRef.slice(from, bytesRef.length() - from);
    }

    private static int findNextZeroByte(BytesReference bytesRef, int from) {
        for (int i = from; i < bytesRef.length(); ++i) {
            if (bytesRef.get(i) == 0) {
                return i;
            }
        }
        return -1;
    }
}

