/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchBoundaryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.ConfigurationBuilder;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests compatibility validation that must happen before a remote exchange is opened.
 */
public class DataNodeComputeHandlerTests extends ESTestCase {

    public void testRejectsUnsupportedFetchBoundary() {
        TransportVersion unsupported = TransportVersionUtils.getPreviousVersion(FetchBoundaryExec.ESQL_FETCH_BOUNDARY);

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> DataNodeComputeHandler.validateFetchBoundaryCompatibility(fetchPlan(), unsupported, "old-node")
        );

        assertThat(
            exception.getMessage(),
            equalTo(
                "fetch boundary requires transport version ["
                    + FetchBoundaryExec.ESQL_FETCH_BOUNDARY
                    + "] but node [old-node] has ["
                    + unsupported
                    + "]"
            )
        );
    }

    public void testAllowsPlanWithoutFetchBoundaryOnUnsupportedTransportVersion() {
        PhysicalPlan plan = new ExchangeSinkExec(Source.EMPTY, List.of(), false, new ExchangeSourceExec(Source.EMPTY, List.of(), false));
        DataNodeComputeHandler.validateFetchBoundaryCompatibility(
            plan,
            TransportVersionUtils.getPreviousVersion(FetchBoundaryExec.ESQL_FETCH_BOUNDARY),
            "old-node"
        );
    }

    public void testAllowsFetchBoundaryOnSupportedTransportVersion() {
        DataNodeComputeHandler.validateFetchBoundaryCompatibility(fetchPlan(), TransportVersion.current(), "current-node");
    }

    public void testRetainedFetchFailsClosedWhenPartialResultsAreRequested() {
        Configuration configuration = new ConfigurationBuilder(EsqlTestUtils.TEST_CFG).allowPartialResults(true).build();

        assertFalse(DataNodeComputeHandler.allowPartialResults(configuration, true));
        assertTrue(DataNodeComputeHandler.allowPartialResults(configuration, false));
    }

    private static PhysicalPlan fetchPlan() {
        Attribute handle = new ReferenceAttribute(Source.EMPTY, null, "_fetch_handle", DataType.KEYWORD);
        FetchBoundaryExec boundary = new FetchBoundaryExec(
            Source.EMPTY,
            new ExchangeSourceExec(Source.EMPTY, List.of(), false),
            handle,
            List.of(handle)
        );
        return new ExchangeSinkExec(Source.EMPTY, boundary.output(), false, boundary);
    }
}
