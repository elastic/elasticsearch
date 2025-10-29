/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.test.ESIntegTestCase;

@ESIntegTestCase.ClusterScope(numDataNodes = 1)
public class EsqlReductionLateMaterializationSingleNodeIT extends EsqlReductionLateMaterializationTestCase {
    public EsqlReductionLateMaterializationSingleNodeIT(TestCase testCase) {
        super(testCase);
    }
}
