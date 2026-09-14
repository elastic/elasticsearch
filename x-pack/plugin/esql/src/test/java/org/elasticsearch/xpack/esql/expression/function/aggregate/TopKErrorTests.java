/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.List;

public class TopKErrorTests extends AbstractTopKErrorTests {
    public TopKErrorTests() {
        super((source, args) -> new TopK(source, args.get(0), args.get(1)));
    }

    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(TopKTests.parameters());
    }
}
