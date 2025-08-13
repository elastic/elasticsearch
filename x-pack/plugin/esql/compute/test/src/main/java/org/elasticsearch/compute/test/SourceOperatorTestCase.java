/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test;

import java.util.Map;

import static org.hamcrest.Matchers.hasKey;

public abstract class SourceOperatorTestCase extends AnyOperatorTestCase {
    @Override
    protected void assertEmptyStatus(Map<String, Object> map) {
        assertThat(map, hasKey("pages_emitted"));
        assertThat(map, hasKey("rows_emitted"));
    }
}
