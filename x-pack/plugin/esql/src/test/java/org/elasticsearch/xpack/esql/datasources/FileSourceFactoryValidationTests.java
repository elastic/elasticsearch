/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;

/**
 * Pins {@link FileSourceFactory#COORDINATOR_KEYS} membership against the keys other code in this
 * factory actually consumes. A missing entry would cause a real WITH-clause option (e.g.
 * {@code error_mode}) to be flagged as unknown for every user. The generic validator contract
 * lives in {@code WithClauseValidatorTests}.
 */
public class FileSourceFactoryValidationTests extends ESTestCase {

    public void testCoordinatorKeysIncludesFormatOverride() {
        assertTrue(FileSourceFactory.COORDINATOR_KEYS.contains(FileSourceFactory.CONFIG_FORMAT));
    }

    public void testCoordinatorKeysIncludesAllErrorPolicyKeys() {
        for (String key : ErrorPolicy.CONFIG_KEYS) {
            assertTrue("ErrorPolicy key " + key + " must be a coordinator key", FileSourceFactory.COORDINATOR_KEYS.contains(key));
        }
    }
}
