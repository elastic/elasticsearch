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
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.io.Reader;

/**
 * A {@link CharFilter} that caps how many characters may be read from the reader it wraps. Once more than
 * {@code maxCharCount} characters have been read, the request fails with a {@code 400}.
 *
 * <p>The {@code _analyze} API applies character filters as a chain, where each filter's output is the next filter's
 * input, so a chain can expand its input far beyond the original text. Wrapping the reader downstream analysis pulls
 * from bounds that expansion.
 *
 * <p>It extends {@link CharFilter} so offset correction is preserved: a tokenizer corrects offsets only through an
 * input that is a {@link CharFilter}. This reader adds no correction of its own and forwards to the wrapped reader.
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
                    + " This limit can be set by changing the ["
                    + IndexSettings.MAX_ANALYZE_CHAR_COUNT_SETTING.getKey()
                    + "] index level setting.",
                RestStatus.BAD_REQUEST
            );
        }
    }
}
