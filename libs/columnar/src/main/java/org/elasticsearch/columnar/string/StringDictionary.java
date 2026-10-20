/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * The terms of a string column's dictionary, in term order. The ordinals are dense, from zero up to {@link #size()},
 * so a caller can index an array by one. A value the dictionary does not name has no ordinal here: the column hands it
 * over as bytes.
 */
public interface StringDictionary {

    /** How many terms the dictionary holds. */
    int size();

    /**
     * The term at {@code ordinal}. The returned {@link BytesRef} is {@code dst}, and is only valid until the next call. The
     * lookup holds no state about any document.
     */
    BytesRef term(int ordinal, BytesRef dst) throws IOException;
}
