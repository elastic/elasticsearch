/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.topn.TopNOperatorStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import static org.hamcrest.Matchers.equalTo;

public class NamedWriteablesTests extends ESTestCase {

    public void testTopNStatus() throws Exception {
        try (EsqlPlugin plugin = new EsqlPlugin()) {
            NamedWriteableRegistry registry = new NamedWriteableRegistry(plugin.getNamedWriteables());
            TopNOperatorStatus origin = new TopNOperatorStatus(
                randomNonNegativeInt(),
                randomNonNegativeLong(),
                randomNonNegativeInt(),
                randomNonNegativeInt(),
                randomNonNegativeLong(),
                randomNonNegativeLong()
            );
            TopNOperatorStatus copy = (TopNOperatorStatus) copyNamedWriteable(origin, registry, Operator.Status.class);
            assertThat(copy.occupiedRows(), equalTo(origin.occupiedRows()));
            assertThat(copy.ramBytesUsed(), equalTo(origin.ramBytesUsed()));
            assertThat(copy.pagesReceived(), equalTo(origin.pagesReceived()));
            assertThat(copy.pagesEmitted(), equalTo(origin.pagesEmitted()));
            assertThat(copy.rowsReceived(), equalTo(origin.rowsReceived()));
            assertThat(copy.rowsEmitted(), equalTo(origin.rowsEmitted()));
        }
    }
}
