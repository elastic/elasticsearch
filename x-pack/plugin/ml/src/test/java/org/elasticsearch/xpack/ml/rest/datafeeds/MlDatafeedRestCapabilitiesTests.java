/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.rest.datafeeds;

import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

public class MlDatafeedRestCapabilitiesTests extends ESTestCase {

    /**
     * Unlike the {@code xpack.ml.esql_datafeeds.enabled} cluster/project setting it replaced,
     * {@link MachineLearning#ESQL_DATAFEEDS_FEATURE_FLAG} is fixed for the process lifetime and cannot be toggled
     * per-cluster/per-project at runtime, so the capability-selection logic in {@link MlDatafeedRestCapabilities}
     * is exercised directly with both boolean values rather than via a mutable cluster/project state.
     */
    public void testSupportedCapabilitiesReflectsEsqlDatafeedsFlag() {
        assertThat(MlDatafeedRestCapabilities.supportedCapabilities(false, false), empty());
        assertThat(
            MlDatafeedRestCapabilities.supportedCapabilities(false, true),
            contains(MlDatafeedRestCapabilities.ML_DATAFEED_ESQL_QUERY)
        );
        assertThat(
            MlDatafeedRestCapabilities.supportedCapabilities(true, false),
            contains(MlDatafeedRestCapabilities.ML_CROSS_PROJECT_SEARCH)
        );
        assertThat(
            MlDatafeedRestCapabilities.supportedCapabilities(true, true),
            containsInAnyOrder(MlDatafeedRestCapabilities.ML_CROSS_PROJECT_SEARCH, MlDatafeedRestCapabilities.ML_DATAFEED_ESQL_QUERY)
        );
    }

    /**
     * Smoke test that the REST handlers wire {@code supportedCapabilities()} through to
     * {@link MachineLearning#ESQL_DATAFEEDS_FEATURE_FLAG}. The flag is fixed for this process, so this only
     * observes whichever value is currently in effect (enabled by default in snapshot/test builds); the
     * capability-selection logic itself is covered for both values above.
     */
    public void testRestHandlersReflectCurrentEsqlDatafeedsFlagState() {
        boolean esqlDatafeedsEnabled = MachineLearning.ESQL_DATAFEEDS_FEATURE_FLAG.isEnabled();
        List<RestHandler> handlers = handlers(true);
        for (RestHandler handler : handlers) {
            if (esqlDatafeedsEnabled) {
                assertThat(
                    handler.supportedCapabilities(),
                    containsInAnyOrder(
                        MlDatafeedRestCapabilities.ML_CROSS_PROJECT_SEARCH,
                        MlDatafeedRestCapabilities.ML_DATAFEED_ESQL_QUERY
                    )
                );
            } else {
                assertThat(handler.supportedCapabilities(), contains(MlDatafeedRestCapabilities.ML_CROSS_PROJECT_SEARCH));
            }
        }
    }

    private static List<RestHandler> handlers(boolean mlCrossProjectSearchEnabled) {
        return List.of(
            new RestPutDatafeedAction(mlCrossProjectSearchEnabled),
            new RestUpdateDatafeedAction(mlCrossProjectSearchEnabled),
            new RestPreviewDatafeedAction(mlCrossProjectSearchEnabled)
        );
    }
}
