/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.health.RestGetHealthAction.CAPABILITY_STATELESS;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class RestGetHealthActionTests extends ESTestCase {

    public void testHealthReportAPIDoesNotTripCircuitBreakers() {
        assertThat(new RestGetHealthAction().canTripCircuitBreaker(), is(false));
    }

    public void testAdvertisesStatelessCapabilityWhenStateless() {
        Settings settings = Settings.builder().put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true).build();
        assertThat(new RestGetHealthAction(settings).supportedCapabilities(), hasItem(CAPABILITY_STATELESS));
    }

    public void testDoesNotAdvertiseStatelessCapabilityWhenStateful() {
        assertThat(new RestGetHealthAction().supportedCapabilities(), not(hasItem(CAPABILITY_STATELESS)));
    }
}
