/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.elasticsearch.xcontent.Text;

/**
 * Base {@link SourceColumnCursor} whose value accessors default to throwing. A homogeneous column only
 * ever reports one {@link org.elasticsearch.eirf.EirfType} from {@link #type()}, so its cursor overrides
 * just the matching accessor; the others stay unreachable. {@link #advance()} and {@link #type()} remain
 * abstract.
 */
public abstract class AbstractSourceColumnCursor implements SourceColumnCursor {

    @Override
    public long longValue() {
        throw unsupported("long");
    }

    @Override
    public double doubleValue() {
        throw unsupported("double");
    }

    @Override
    public Text stringValue() {
        throw unsupported("string");
    }

    private IllegalStateException unsupported(String what) {
        return new IllegalStateException("cursor at this position has no " + what + " value");
    }
}
