/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.iplocation.api.IpLocationConsumer;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

public class RequestIpLocationDownloadsActionRequestTests extends AbstractWireSerializingTestCase<
    RequestIpLocationDownloadsAction.Request> {

    @Override
    protected Writeable.Reader<RequestIpLocationDownloadsAction.Request> instanceReader() {
        return RequestIpLocationDownloadsAction.Request::new;
    }

    @Override
    protected RequestIpLocationDownloadsAction.Request createTestInstance() {
        return new RequestIpLocationDownloadsAction.Request(
            TEST_REQUEST_TIMEOUT,
            randomProjectIdOrDefault(),
            randomFrom(IpLocationConsumer.values()),
            randomFrom(RequestIpLocationDownloadsAction.Request.Operation.values())
        );
    }

    @Override
    protected RequestIpLocationDownloadsAction.Request mutateInstance(RequestIpLocationDownloadsAction.Request instance) {
        ProjectId projectId = instance.getProjectId();
        IpLocationConsumer consumer = instance.getConsumer();
        RequestIpLocationDownloadsAction.Request.Operation operation = instance.operation();
        switch (between(0, 2)) {
            case 0 -> projectId = randomValueOtherThan(projectId, ESTestCase::randomProjectIdOrDefault);
            case 1 -> consumer = randomValueOtherThan(consumer, () -> randomFrom(IpLocationConsumer.values()));
            case 2 -> operation = randomValueOtherThan(
                operation,
                () -> randomFrom(RequestIpLocationDownloadsAction.Request.Operation.values())
            );
            default -> throw new AssertionError("unknown switch branch");
        }
        return new RequestIpLocationDownloadsAction.Request(instance.masterNodeTimeout(), projectId, consumer, operation);
    }

    @Override
    protected void assertEqualInstances(
        RequestIpLocationDownloadsAction.Request expectedInstance,
        RequestIpLocationDownloadsAction.Request newInstance
    ) {
        assertEquals(expectedInstance.masterNodeTimeout(), newInstance.masterNodeTimeout());
        assertEquals(expectedInstance.getProjectId(), newInstance.getProjectId());
        assertEquals(expectedInstance.getConsumer(), newInstance.getConsumer());
        assertEquals(expectedInstance.operation(), newInstance.operation());
    }
}
