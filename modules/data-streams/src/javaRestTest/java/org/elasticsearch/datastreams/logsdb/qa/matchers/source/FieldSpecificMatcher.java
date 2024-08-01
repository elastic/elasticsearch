/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.logsdb.qa.matchers.source;

import org.apache.lucene.sandbox.document.HalfFloatPoint;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface FieldSpecificMatcher {
    boolean match(List<Object> actual, List<Object> expected);

    class HalfFloatMatcher implements FieldSpecificMatcher {
        public boolean match(List<Object> actual, List<Object> expected) {
            var actualHalfFloatBytes = normalize(actual);
            var expectedHalfFloatBytes = normalize(expected);

            return actualHalfFloatBytes.equals(expectedHalfFloatBytes);
        }

        private static Set<Short> normalize(List<Object> values) {
            if (values == null) {
                return Set.of();
            }

            Function<Object, Float> toFloat = (o) -> o instanceof Number n ? n.floatValue() : Float.parseFloat((String) o);
            return values.stream()
                .filter(Objects::nonNull)
                .map(toFloat)
                // Based on logic in NumberFieldMapper
                .map(HalfFloatPoint::halfFloatToSortableShort)
                .collect(Collectors.toSet());
        }
    }
}
