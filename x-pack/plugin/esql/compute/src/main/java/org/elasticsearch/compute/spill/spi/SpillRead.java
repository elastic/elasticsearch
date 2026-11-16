/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill.spi;

import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.core.Nullable;

/**
 * The result of one read: data the caller owns and closes, or the end of the range, which is distinct from empty data.
 */
public record SpillRead(@Nullable ReleasableBytesReference data) {

    private static final SpillRead EOF = new SpillRead(null);

    public static SpillRead eof() {
        return EOF;
    }

    public static SpillRead of(ReleasableBytesReference data) {
        return new SpillRead(data);
    }

    public boolean isEof() {
        return data == null;
    }
}
