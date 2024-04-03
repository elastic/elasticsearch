/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ccr.action;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

public class CcrStatsActionTests extends AbstractWireSerializingTestCase<CcrStatsAction.Request> {

    @Override
    protected Writeable.Reader<CcrStatsAction.Request> instanceReader() {
        return CcrStatsAction.Request::new;
    }

    @Override
    protected CcrStatsAction.Request createTestInstance() {
        var request = new CcrStatsAction.Request();
        request.setTimeout(randomFrom("1s", "5s", "10s", "15s"));
        request.masterNodeTimeout(randomFrom("1s", "5s", "10s", "15s"));
        return request;
    }

    @Override
    protected CcrStatsAction.Request mutateInstance(CcrStatsAction.Request instance) throws IOException {
        return switch (randomInt(1)) {
            case 0 -> {
                var mutatedInstance = new CcrStatsAction.Request();
                mutatedInstance.setTimeout(instance.getTimeout());
                mutatedInstance.masterNodeTimeout(randomFrom("20s", "25s", "30s"));
                yield mutatedInstance;
            }
            case 1 -> {
                var mutatedInstance = new CcrStatsAction.Request();
                mutatedInstance.setTimeout(randomFrom("20s", "25s", "30s"));
                mutatedInstance.masterNodeTimeout(instance.masterNodeTimeout());
                yield mutatedInstance;
            }
            default -> throw new RuntimeException("Cannot happen");
        };
    }

    public void testSerializationBwc() throws IOException {
        // In previous version `timeout` is not set
        var request = new CcrStatsAction.Request();
        if (randomBoolean()) {
            request.masterNodeTimeout(randomFrom("20s", "25s", "30s"));
        }
        assertSerialization(request, TransportVersionUtils.getPreviousVersion(TransportVersions.CCR_STATS_API_TIMEOUT_PARAM));
        assertSerialization(request, TransportVersions.MINIMUM_CCS_VERSION);
    }
}
