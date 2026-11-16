/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class ComputeServiceRootOfTests extends ESTestCase {

    public void testRootOfASessionIdIsItself() {
        String session = UUIDs.randomBase64UUID();
        assertThat(ComputeService.rootOf(session), equalTo(session));
    }

    public void testRootOfAChildSessionIsTheRoot() {
        String session = UUIDs.randomBase64UUID();
        assertThat(ComputeService.rootOf(session + "/" + between(1, 1000)), equalTo(session));
        assertThat("only the first suffix matters", ComputeService.rootOf(session + "/3/7"), equalTo(session));
    }

    public void testRootOfARemoteClusterSessionDropsTheAlias() {
        String session = UUIDs.randomBase64UUID();
        String alias = randomAlphaOfLengthBetween(1, 10);
        assertThat(ComputeService.rootOf(alias + ":" + session), equalTo(session));
        assertThat(ComputeService.rootOf(alias + ":" + session + "/" + between(1, 100)), equalTo(session));
    }
}
