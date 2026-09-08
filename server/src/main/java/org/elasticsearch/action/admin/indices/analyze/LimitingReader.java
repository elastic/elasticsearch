/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.indices.analyze;

import org.apache.lucene.analysis.CharFilter;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.io.Reader;

/**
 * A {@link CharFilter} that caps how many characters may be read from the reader it wraps. Once more than
 * {@code maxCharCount} characters have been read the request fails with a {@code 400} rather than continuing to buffer
 * characters in memory.
 *
 * <p>The {@code _analyze} API applies character filters as a chain, where each filter's output is the next filter's
 * input. Such a chain can expand its input far beyond the original text, so wrapping the reader that downstream analysis
 * pulls from bounds that expansion and keeps a single request from exhausting the node's heap.
 *
 * <p>It extends {@link CharFilter} rather than a plain reader so that offset correction is preserved: a tokenizer asks
 * its input to correct token offsets only when that input is a {@link CharFilter}. This reader applies no correction of
 * its own and forwards each request to the reader it wraps, so an underlying character filter's offsets still reach the
 * tokenizer.
 */
final class LimitingReader extends CharFilter {

    private final int maxCharCount;
    private long charCount;

    LimitingReader(Reader in, int maxCharCount) {
        super(in);
        this.maxCharCount = maxCharCount;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        final int read = input.read(cbuf, off, len);
        if (read > 0) {
            increment(read);
        }
        return read;
    }

    @Override
    protected int correct(int currentOff) {
        return currentOff;
    }

    private void increment(int count) {
        charCount += count;
        if (charCount > maxCharCount) {
            throw new ElasticsearchStatusException(
                "The number of characters produced by calling _analyze has exceeded the allowed maximum of ["
                    + maxCharCount
                    + "]."
                    + " This limit can be set by changing the [index.analyze.max_char_count] index level setting.",
                RestStatus.BAD_REQUEST
            );
        }
    }
}
