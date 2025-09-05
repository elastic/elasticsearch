/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.index.IndexSettings.IGNORE_ABOVE_DEFAULT_STANDARD_INDICES;

public class ParameterTests extends ESTestCase {

    public void test_ignore_above_param_default() {
        // when
        FieldMapper.Parameter<Integer> ignoreAbove = FieldMapper.Parameter.ignoreAboveParam((FieldMapper fm) -> 123);

        // then
        assertEquals(IGNORE_ABOVE_DEFAULT_STANDARD_INDICES, ignoreAbove.getValue().intValue());
    }

    public void test_ignore_above_param_invalid_value() {
        // when
        FieldMapper.Parameter<Integer> ignoreAbove = FieldMapper.Parameter.ignoreAboveParam((FieldMapper fm) -> -1);
        ignoreAbove.setValue(-1);

        // then
        assertThrows(IllegalArgumentException.class, ignoreAbove::validate);
    }

}
