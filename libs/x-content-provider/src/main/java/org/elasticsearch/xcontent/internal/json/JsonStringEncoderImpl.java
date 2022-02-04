/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.internal.json;

import org.elasticsearch.xcontent.json.JsonStringEncoder;

public final class JsonStringEncoderImpl extends JsonStringEncoder {

    private final com.fasterxml.jackson.core.io.JsonStringEncoder delegate;

    public static JsonStringEncoderImpl getInstance() {
        return new JsonStringEncoderImpl();
    }

    private JsonStringEncoderImpl() {
        delegate = com.fasterxml.jackson.core.io.JsonStringEncoder.getInstance();
    }

    @Override
    public byte[] quoteAsUTF8(String text) {
        return delegate.quoteAsUTF8(text);
    }

    @Override
    public char[] quoteAsString(CharSequence input) {
        return delegate.quoteAsString(input);
    }

    @Override
    public char[] quoteAsString(String input) {
        return delegate.quoteAsString(input);
    }

    public void quoteAsString(CharSequence input, StringBuilder output) {
        delegate.quoteAsString(input, output);
    }
}
