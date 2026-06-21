/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.index.mapper.BinaryDVBlockLoaderTestCase;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MatchOnlyTextFieldBlockLoaderTests extends BinaryDVBlockLoaderTestCase {

    public MatchOnlyTextFieldBlockLoaderTests(Params params) {
        super(FieldType.MATCH_ONLY_TEXT.toString(), params);
    }

    @Override
    protected Object expected(Map<String, Object> fieldMapping, Object value, TestContext testContext) {
        // match_only_text enables doc values either when the single-value handler forces them on or by default in strict-columnar mode
        boolean strictColumnar = params.indexMode().isStrictColumnar();
        if (hasDocValues(fieldMapping, strictColumnar)) {
            return strictColumnar ? valuesInSourceOrder(value) : valuesInSortedOrder(value);
        }

        // Without doc values, a genuinely synthetic source reconstructs the field from a per-field fallback. Multi fields don't get that
        // fallback, so nothing can be loaded for them. Columnar-stored source, by contrast, keeps a real stored _source blob, so a multi
        // field still resolves to its parent's reconstructed value. Either way the binary fallback returns sorted order while the stored
        // fallback preserves source order.
        if (params.syntheticSource() || params.isColumnarStored()) {
            if (testContext.isMultifield() && params.syntheticSource()) {
                return null;
            }
            return params.binaryDocValues() ? valuesInSortedOrder(value) : valuesInSourceOrder(value);
        }

        // Non-synthetic source loads the value back from stored _source in its original order.
        return valuesInSourceOrder(value);
    }

    @SuppressWarnings("unchecked")
    private static Object valuesInSortedOrder(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String s) {
            return new BytesRef(s);
        }
        var resultList = ((List<String>) value).stream().filter(Objects::nonNull).map(BytesRef::new).sorted().toList();
        return maybeFoldList(resultList);
    }

    @SuppressWarnings("unchecked")
    private static Object valuesInSourceOrder(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String s) {
            return new BytesRef(s);
        }
        var resultList = ((List<String>) value).stream().filter(Objects::nonNull).map(BytesRef::new).toList();
        return maybeFoldList(resultList);
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
    }
}
