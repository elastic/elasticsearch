/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.benchmark._nightly.esql;

import org.elasticsearch.test.ESTestCase;

public class ParquetReadBenchmarkTests extends ESTestCase {
    public void test() {
        ParquetReadBenchmark.selfTest();
    }
}
