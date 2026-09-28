/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.rest.datafeeds;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

public class MlDatafeedRestCapabilitiesTests extends ESTestCase {

    public void testPutDatafeedActionShouldAdvertiseCapabilityWhenMlCpsEnabled() {
        assertThat(new RestPutDatafeedAction(true).supportedCapabilities(), contains(MlDatafeedRestCapabilities.ML_CROSS_PROJECT_SEARCH));
    }

    public void testPutDatafeedActionShouldNotAdvertiseCapabilityWhenMlCpsDisabled() {
        assertThat(new RestPutDatafeedAction(false).supportedCapabilities(), empty());
    }

    public void testUpdateDatafeedActionShouldAdvertiseCapabilityWhenMlCpsEnabled() {
        assertThat(
            new RestUpdateDatafeedAction(true).supportedCapabilities(),
            contains(MlDatafeedRestCapabilities.ML_CROSS_PROJECT_SEARCH)
        );
    }

    public void testUpdateDatafeedActionShouldNotAdvertiseCapabilityWhenMlCpsDisabled() {
        assertThat(new RestUpdateDatafeedAction(false).supportedCapabilities(), empty());
    }

    public void testPreviewDatafeedActionShouldAdvertiseCapabilityWhenMlCpsEnabled() {
        assertThat(
            new RestPreviewDatafeedAction(true).supportedCapabilities(),
            contains(MlDatafeedRestCapabilities.ML_CROSS_PROJECT_SEARCH)
        );
    }

    public void testPreviewDatafeedActionShouldNotAdvertiseCapabilityWhenMlCpsDisabled() {
        assertThat(new RestPreviewDatafeedAction(false).supportedCapabilities(), empty());
    }
}
