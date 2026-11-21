/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;

public class EscfDoubleArrayIteratorTests extends AbstractEscfNumericArrayIteratorTestCase {

    @Override
    protected EscfNumericArrayIterator iterator(EscfColumn column, Function<String, ? extends RuntimeException> onUnsupported) {
        return EscfDoubleArrayIterator.of(column, onUnsupported);
    }

    @Override
    protected long value(EscfNumericArrayIterator iterator) {
        return Double.doubleToLongBits(((EscfDoubleArrayIterator) iterator).value());
    }

    @Override
    protected long xContentValue(XContentParser parser) throws IOException {
        return Double.doubleToLongBits(parser.doubleValue());
    }

    /**
     * The one place the iterator is stricter than x-content: an integer beyond the long range is stored as a string,
     * indistinguishable from a quoted one, so it fails even though {@code doubleValue()} would read it.
     */
    public void testIntegerBeyondLongRangeFails() throws IOException {
        final List<String> sources = List.of("{\"f\":[123456789012345678901234567890]}");
        assertThat(xContent(sources), equalTo(List.of(new Row(0, List.of(Double.doubleToLongBits(1.2345678901234568e29)), false))));
        try (EscfBatch batch = encode(sources)) {
            assertThat(escf(column(batch)), equalTo(List.of(new Row(0, List.of(), true))));
        }
    }
}
