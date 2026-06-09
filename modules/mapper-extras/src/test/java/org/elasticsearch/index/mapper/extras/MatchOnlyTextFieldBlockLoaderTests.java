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
        // match_only_text only enables doc values when the single-value handler forces them on, in which case the document is
        // single-valued; doc values come back in sorted order, which for a single value is also its source order.
        if (hasDocValues(fieldMapping, false)) {
            return valuesInSortedOrder(value);
        }

        // Without doc values, a genuinely synthetic source reconstructs the field from a per-field fallback. Multi fields don't get that
        // fallback, so nothing can be loaded for them. With the binary fallback (binaryDocValues=true), values come back in sorted order;
        // with the stored fallback (binaryDocValues=false), values come back in source order.
        if (params.syntheticSource()) {
            if (testContext.isMultifield()) {
                return null;
            }
            return params.binaryDocValues() ? valuesInSortedOrder(value) : valuesInSourceOrder(value);
        }

        // Columnar-stored source stores every leaf as a per-field _ignored_source entry. The block loader reads from doc values in
        // arrival order (offsets are recorded for both SYNTHETIC and COLUMNAR_STORED source modes in strict-columnar mode) or falls
        // back to the reconstructed _source, which also preserves source/arrival order. Either way, arrival order is returned.
        if (params.isColumnarStored()) {
            return valuesInSourceOrder(value);
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
