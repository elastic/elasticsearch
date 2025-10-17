/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.test.ESTestCase;

import java.net.ConnectException;

import static org.hamcrest.Matchers.startsWith;

public class CsvTestsDataLoaderTests extends ESTestCase {

    public void testCsvTestsDataLoaderExecution() {
        ConnectException ce = expectThrows(ConnectException.class, () -> CsvTestsDataLoader.main(new String[] {}));
        assertThat(ce.getMessage(), startsWith("Connection refused"));
    }
}
