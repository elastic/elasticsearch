/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class TimestampParsingException extends ElasticsearchException {

    private final String timestamp;

    public TimestampParsingException(String timestamp) {
        super("failed to parse timestamp [" + timestamp + "]");
        this.timestamp = timestamp;
    }

    public TimestampParsingException(String timestamp, Throwable cause) {
        super("failed to parse timestamp [" + timestamp + "]", cause);
        this.timestamp = timestamp;
    }

    public String timestamp() {
        return timestamp;
    }

    public TimestampParsingException(StreamInput in) throws IOException {
        super(in);
        this.timestamp = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(timestamp);
    }
}
