/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;

/**
 * The {@code TS_INFO} limit is on the wire under two transport versions: {@code esql_ts_info_limit} on main and the
 * serverless patch branch, and {@code esql_ts_info_limit_backport} on the 9.6 and 9.5 backports.
 */
public class TsInfoLimitOnWireTests extends ESTestCase {

    public void testCurrentVersion() {
        assertTrue(TsInfo.limitOnWire(TransportVersion.current()));
    }

    public void testFirstVersionOnMain() {
        assertTrue(TsInfo.limitOnWire(TransportVersion.fromName("esql_ts_info_limit")));
    }

    public void testServerlessPatch() {
        assertTrue(TsInfo.limitOnWire(TransportVersion.fromId(9616001)));
    }

    public void testBackports() {
        assertTrue(TsInfo.limitOnWire(TransportVersion.fromId(9610006)));
        assertTrue(TsInfo.limitOnWire(TransportVersion.fromId(9458022)));
    }

    public void testReleasesBeforeTheBackports() {
        assertFalse(TsInfo.limitOnWire(TransportVersion.fromId(9610005)));
        assertFalse(TsInfo.limitOnWire(TransportVersion.fromId(9458021)));
    }

    public void testMainBeforeTheLimit() {
        assertFalse(TsInfo.limitOnWire(TransportVersion.fromId(9619000)));
    }
}
