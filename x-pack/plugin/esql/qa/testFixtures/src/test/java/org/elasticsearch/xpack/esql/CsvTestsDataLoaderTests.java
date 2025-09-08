/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.test.ESTestCase;

import java.net.ConnectException;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

public class CsvTestsDataLoaderTests extends ESTestCase {

    public void testCsvTestsDataLoaderExecution() {
        Throwable cause = expectThrows(AssertionError.class, () -> CsvTestsDataLoader.main(new String[] {}));
        // find the root cause
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        assertThat(cause, instanceOf(ConnectException.class));
        assertThat(cause.getMessage(), startsWith("Connection refused"));
    }
}
