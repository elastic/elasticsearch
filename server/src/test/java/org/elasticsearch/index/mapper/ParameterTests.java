/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.index.IndexSettings.IGNORE_ABOVE_DEFAULT;
import static org.elasticsearch.index.IndexSettings.IGNORE_ABOVE_DEFAULT_LOGSDB;

public class ParameterTests extends ESTestCase {

    public void test_ignore_above_param_default() {
        // when
        FieldMapper.Parameter<Integer> ignoreAbove = FieldMapper.Parameter.ignoreAboveParam((FieldMapper fm) -> 123);

        // then
        assertEquals(IGNORE_ABOVE_DEFAULT, ignoreAbove.getValue().intValue());
    }

    public void test_ignore_above_param_default_for_standard_indices() {
        // when
        FieldMapper.Parameter<Integer> ignoreAbove = FieldMapper.Parameter.ignoreAboveParam(
            (FieldMapper fm) -> 123,
            IndexMode.STANDARD,
            IndexVersion.current()
        );

        // then
        assertEquals(IGNORE_ABOVE_DEFAULT, ignoreAbove.getValue().intValue());
    }

    public void test_ignore_above_param_default_for_logsdb_indices() {
        // when
        FieldMapper.Parameter<Integer> ignoreAbove = FieldMapper.Parameter.ignoreAboveParam(
            (FieldMapper fm) -> 123,
            IndexMode.LOGSDB,
            IndexVersion.current()
        );

        // then
        assertEquals(IGNORE_ABOVE_DEFAULT_LOGSDB, ignoreAbove.getValue().intValue());
    }

    public void test_ignore_above_param_invalid_value() {
        // when
        FieldMapper.Parameter<Integer> ignoreAbove = FieldMapper.Parameter.ignoreAboveParam((FieldMapper fm) -> -1);
        ignoreAbove.setValue(-1);

        // then
        assertThrows(IllegalArgumentException.class, ignoreAbove::validate);
    }

}
