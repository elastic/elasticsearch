/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

/**
 * Round-trips synthetic {@code _source} for high-cardinality text fields in strictly columnar mode, which store their values in document
 * order with inline nulls ({@link MultiValuedBinaryDocValuesField.ArrayOrderInlineNull}) instead of a sidecar {@code .offsets} field. The
 * value-exceeding-max-term-length case is covered by {@code TextFieldMapperTests#testColumnarArrayOrderWithValueExceedMaxTermLength}.
 */
public class TextColumnarArrayOrderSyntheticSourceTests extends AbstractColumnarArrayOrderSyntheticSourceTestCase {

    @Override
    protected String fieldTypeName() {
        return "text";
    }
}
