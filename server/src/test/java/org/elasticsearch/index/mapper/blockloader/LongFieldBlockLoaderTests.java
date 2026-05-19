/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader;

import org.elasticsearch.common.Numbers;
import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.index.mapper.NumberFieldBlockLoaderTestCase;

import java.util.Map;

public class LongFieldBlockLoaderTests extends NumberFieldBlockLoaderTestCase<Long> {
    public LongFieldBlockLoaderTests(Params params) {
        super(FieldType.LONG, params);
    }

    @Override
    protected Long convert(Number value, Map<String, Object> fieldMapping) {
        return value.longValue();
    }

    /**
     * Overrides the default {@link Double#parseDouble}-based implementation to match the long
     * mapper's actual indexing behavior.
     *
     * <p>During indexing, {@code AbstractXContentParser.toLong} tries {@link Long#parseLong} first,
     * which accepts Unicode digits. This is broader than what {@link Double#parseDouble} accepts.
     *
     * <p>Note: {@link #tryParseStringFromSource} is intentionally left at the default
     * ({@link Double#parseDouble}) because the stored-source re-parsing path goes through
     * {@code objectToLong -> objectToDouble -> Double.parseDouble}, which rejects those same Unicode
     * digits. This asymmetry between indexing and stored-source re-parsing is a known inconsistency
     * in the production code.
     */
    @Override
    protected Number tryParseString(String s) {
        try {
            return Numbers.toLong(s, true);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
