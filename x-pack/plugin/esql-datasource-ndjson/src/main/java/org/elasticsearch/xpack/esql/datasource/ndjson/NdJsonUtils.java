/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;

class NdJsonUtils {
    static final JsonFactory JSON_FACTORY = new JsonFactory();

    /**
     * Given a parser and the stream it reads from, restart parsing at the next line.
     * @param parser the JSON parser
     * @param input the stream the parser reads from
     * @return a new stream to read from
     */
    static InputStream moveToNextLine(JsonParser parser, InputStream input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        parser.releaseBuffered(baos);
        parser.close();
        if (baos.size() > 0) {
            // FIXME: improve this by using a single byte buffer and the actual stream
            // The code below will create a list of BAIS for every parsing error encountered.
            input = new SequenceInputStream(new ByteArrayInputStream(baos.toByteArray()), input);
        }

        char c;
        while ((c = (char) input.read()) != -1) {
            if (c == '\n' || c == '\r') {
                break;
            }
        }

        // Note: location will restart at line 1.
        return input;
    }
}
