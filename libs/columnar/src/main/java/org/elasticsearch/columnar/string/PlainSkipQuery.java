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

/** What a term, range or prefix query asks of a plain column, for a skip region to rule entries out. */
record PlainSkipQuery(BytesRef lower, boolean includeLower, BytesRef upper, boolean includeUpper, BytesRef prefix) {

    static PlainSkipQuery range(BytesRef lower, boolean includeLower, BytesRef upper, boolean includeUpper) {
        return new PlainSkipQuery(lower, includeLower, upper, includeUpper, null);
    }

    static PlainSkipQuery prefix(BytesRef prefix) {
        return new PlainSkipQuery(null, false, null, false, prefix);
    }
}
