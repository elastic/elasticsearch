/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.translog;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class TranslogCorruptedException extends ElasticsearchException {
    public TranslogCorruptedException(String source, String details) {
        super(corruptedMessage(source, details));
    }

    public TranslogCorruptedException(String source, Throwable cause) {
        this(source, null, cause);
    }

    public TranslogCorruptedException(String source, String details, Throwable cause) {
        super(corruptedMessage(source, details), cause);
    }

    private static String corruptedMessage(String source, String details) {
        String msg = "translog from source [" + source + "] is corrupted";
        if (details != null) {
            msg += ", " + details;
        }
        return msg;
    }

    public TranslogCorruptedException(StreamInput in) throws IOException {
        super(in);
    }
}
