/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

/**
 * Plain text tabular SQL response
 */
public class PlainResponse {
    public final long tookNanos;
    public final String data;
    public final String cursor;

    public PlainResponse(long tookNanos, String cursor, String data) {
        this.data = data;
        this.tookNanos = tookNanos;
        this.cursor = cursor;
    }
}

