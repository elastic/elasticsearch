/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.postings.terms;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.FeatureFlag;

/**
 * This class is not forked from Lucene and a place to keep non-forked cod.
 */
public final class NonForkedHelper {

    public static final FeatureFlag USE_FORKED_TERMS_READER = new FeatureFlag("use_forked_terms_reader");

    /**
     * Returns the term if length is smaller or equal to 512 bytes other <code>null</code> is returned.
     *
     * Returned <code>null</code> would mean the min or max term would be read from disk each time {@link FieldReader#getMin()}
     * or {@link FieldReader#getMax()} would be invoked.
     *
     * @return Returns the term if length is smaller or equal to 512 bytes other <code>null</code> is returned.
     */
    public static BytesRef shouldKeepMinOrMaxTerm(BytesRef term) {
        if (term.length <= 512) {
            return term;
        } else {
            return null;
        }
    }

    private NonForkedHelper() {}
}
