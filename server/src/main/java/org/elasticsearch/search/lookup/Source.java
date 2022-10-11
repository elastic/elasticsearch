/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.lookup;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xcontent.XContentType;

import java.util.Map;

/**
 * The source of a document.  Note that, while objects returned from {@link #source()}
 * and {@link #internalSourceRef()} are immutable, objects implementing this interface
 * may not be immutable themselves.
 */
public interface Source {

    /**
     * The content type of the source, if stored as bytes
     */
    XContentType sourceContentType();

    /**
     * A map representation of the source
     * <p>
     * Important: This can lose precision on numbers with a decimal point. It
     * converts numbers like {@code "n": 1234.567} to a {@code double} which
     * only has 52 bits of precision in the mantissa. This will come up most
     * frequently when folks write nanosecond precision dates as a decimal
     * number.
     */
    Map<String, Object> source();

    /**
     * A byte representation of the source
     */
    BytesReference internalSourceRef();

    /**
     * Find a value at a given path in the source
     * @param path          the path to locate
     * @param nullValue     an object to return if the value at the path is {@code null}
     */
    Object extractValue(String path, @Nullable Object nullValue);

    /**
     * Apply a filter to this source, returning a new map representation
     */
    Map<String, Object> filter(FetchSourceContext context);
}
