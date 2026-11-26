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
import java.util.function.Function;

public class EscfLongArrayIteratorTests extends AbstractEscfNumericArrayIteratorTestCase {

    @Override
    protected EscfNumericArrayIterator iterator(EscfColumn column, Function<String, ? extends RuntimeException> onUnsupported) {
        return EscfLongArrayIterator.of(column, onUnsupported);
    }

    @Override
    protected long value(EscfNumericArrayIterator iterator) {
        return ((EscfLongArrayIterator) iterator).value();
    }

    @Override
    protected long xContentValue(XContentParser parser) throws IOException {
        return parser.longValue();
    }
}
