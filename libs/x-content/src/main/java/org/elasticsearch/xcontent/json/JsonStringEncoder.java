/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.json;

import org.elasticsearch.xcontent.spi.XContentProvider;

/**
 * Encoder of JSON String values (including JSON field names) into Strings or UTF-8 byte arrays.
 */
public abstract class JsonStringEncoder {

    protected JsonStringEncoder() {}

    private static final JsonStringEncoder INSTANCE = XContentProvider.provider().getJsonStringEncoder();

    /**
     * Factory method for getting an instance.
     */
    public static JsonStringEncoder getInstance() {
        return INSTANCE;
    }

    /**
     * Quotes a given JSON String value using standard quoting, encoding as UTF-8, and return results as a byte array.
     */
    public abstract byte[] quoteAsUTF8(String text);

    /**
     * Quotes text contents using JSON standard quoting, and return results as a character array.
     */
    public abstract char[] quoteAsString(CharSequence input);

    /**
     * Quotes text contents using JSON standard quoting, and return results as a character array.
     */
    public abstract char[] quoteAsString(String input);

    /**
     * Quotes text contents using JSON standard quoting, appending the results to the given {@link StringBuilder}.
     * Use this variant if you have e.g. a {@link StringBuilder} and want to avoid superfluous copying of it.
     */
    public abstract void quoteAsString(CharSequence input, StringBuilder output);

}
