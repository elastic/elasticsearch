/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.io.stream;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import static org.hamcrest.Matchers.equalTo;

public class PlanStreamOutputTests extends ESTestCase {

    public void testTransportVersion() {
        BytesStreamOutput out = new BytesStreamOutput();
        TransportVersion v1 = TransportVersionUtils.randomCompatibleVersion(random());
        out.setTransportVersion(v1);
        PlanStreamOutput planOut = new PlanStreamOutput(out, PlanNameRegistry.INSTANCE);
        assertThat(planOut.getTransportVersion(), equalTo(v1));
        TransportVersion v2 = TransportVersionUtils.randomCompatibleVersion(random());
        planOut.setTransportVersion(v2);
        assertThat(planOut.getTransportVersion(), equalTo(v2));
        assertThat(out.getTransportVersion(), equalTo(v2));
    }
}
