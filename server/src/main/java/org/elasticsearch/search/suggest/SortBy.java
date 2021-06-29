/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * An enum representing the valid sorting options
 */
public enum SortBy implements Writeable {
    /** Sort should first be based on score, then document frequency and then the term itself. */
    SCORE,
    /** Sort should first be based on document frequency, then score and then the term itself. */
    FREQUENCY;

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    public static SortBy readFromStream(final StreamInput in) throws IOException {
        return in.readEnum(SortBy.class);
    }

    public static SortBy resolve(final String str) {
        Objects.requireNonNull(str, "Input string is null");
        return valueOf(str.toUpperCase(Locale.ROOT));
    }
}
