/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.analysis.lucene;

import org.apache.lucene.analysis.CharFilter;
import org.elasticsearch.common.io.Streams;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.UncheckedIOException;

// Simple char filter that appends text to the term
public class AppendCharFilter extends CharFilter {

    static Reader append(Reader input, String appendMe) {
        try {
            return new StringReader(Streams.copyToString(input) + appendMe);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public AppendCharFilter(Reader input, String appendMe) {
        super(append(input, appendMe));
    }

    @Override
    protected int correct(int currentOff) {
        return currentOff;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        return input.read(cbuf, off, len);
    }
}
