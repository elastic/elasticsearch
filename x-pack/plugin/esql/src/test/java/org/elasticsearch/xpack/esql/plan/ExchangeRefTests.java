/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.operator.PlanProfile;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ExchangeRefTests extends ESTestCase {

    public void testIdsAreUnique() {
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            assertTrue("duplicate exchange id", ids.add(ExchangeRef.newId()));
        }
    }

    public void testChildIndexKeepsTheId() {
        ExchangeRef exchange = ExchangeRef.newExchange();
        assertThat(exchange.childIndex(), nullValue());
        ExchangeRef branch = exchange.withChildIndex(2);
        assertThat(branch.id(), equalTo(exchange.id()));
        assertThat(branch.childIndex(), equalTo(2));
        assertThat("the original is not changed", exchange.childIndex(), nullValue());
    }

    public void testWireRoundTrip() throws IOException {
        TransportVersion version = TransportVersionUtils.randomVersionSupporting(PlanProfile.JSON_PLANS_VERSION);
        for (ExchangeRef ref : new ExchangeRef[] {
            ExchangeRef.newExchange(),
            ExchangeRef.newExchange().withChildIndex(randomInt(5)),
            null }) {
            assertThat(roundTrip(ref, version), equalTo(ref));
        }
    }

    /** A node that does not know about exchange ids cannot be sent one, and a plan built for it simply has none. */
    public void testOldPeersGetNoExchange() throws IOException {
        TransportVersion version = TransportVersionUtils.randomVersionNotSupporting(PlanProfile.JSON_PLANS_VERSION);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            ExchangeRef.writeOptionalTo(out, ExchangeRef.newExchange().withChildIndex(1));
            assertThat("nothing is written for an old peer", out.size(), equalTo(0));
        }
        assertThat(roundTrip(ExchangeRef.newExchange(), version), nullValue());
    }

    private static ExchangeRef roundTrip(ExchangeRef ref, TransportVersion version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            ExchangeRef.writeOptionalTo(out, ref);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(version);
                return ExchangeRef.readOptionalFrom(in);
            }
        }
    }
}
