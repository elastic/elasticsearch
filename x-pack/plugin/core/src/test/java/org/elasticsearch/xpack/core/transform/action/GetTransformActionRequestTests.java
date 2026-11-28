/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction.Request;

import static org.elasticsearch.xpack.core.transform.action.GetTransformAction.DANGLING_TASKS;

public class GetTransformActionRequestTests extends AbstractBWCWireSerializationTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        if (randomBoolean()) {
            return new Request(Metadata.ALL);
        }
        return new Request(randomAlphaOfLengthBetween(1, 20), randomBoolean(), randomPositiveTimeValue());
    }

    @Override
    protected Request mutateInstance(Request instance) {
        // Request.equals also includes pageParams and allowNoResources from AbstractGetResourcesRequest.
        // The constructors hardcode PageParams.defaultParams() and allowNoResources == true, so those two
        // are copied back on with the setters after each one-field change.
        return switch (between(0, 4)) {
            case 0 -> copy(
                randomValueOtherThan(instance.getId(), () -> randomAlphaOfLengthBetween(1, 20)),
                instance.checkForDanglingTasks(),
                instance.timeout(),
                instance.getPageParams(),
                instance.isAllowNoResources()
            );
            case 1 -> copy(
                instance.getId(),
                randomValueOtherThan(instance.checkForDanglingTasks(), ESTestCase::randomBoolean),
                instance.timeout(),
                instance.getPageParams(),
                instance.isAllowNoResources()
            );
            case 2 -> copy(
                instance.getId(),
                instance.checkForDanglingTasks(),
                randomValueOtherThan(instance.timeout(), ESTestCase::randomPositiveTimeValue),
                instance.getPageParams(),
                instance.isAllowNoResources()
            );
            case 3 -> copy(
                instance.getId(),
                instance.checkForDanglingTasks(),
                instance.timeout(),
                randomValueOtherThan(instance.getPageParams(), () -> new PageParams(randomIntBetween(0, 5), randomIntBetween(1, 50))),
                instance.isAllowNoResources()
            );
            case 4 -> copy(
                instance.getId(),
                instance.checkForDanglingTasks(),
                instance.timeout(),
                instance.getPageParams(),
                instance.isAllowNoResources() == false
            );
            default -> throw new AssertionError("illegal mutate branch");
        };
    }

    private static Request copy(
        String id,
        boolean checkForDanglingTasks,
        TimeValue timeout,
        PageParams pageParams,
        boolean allowNoResources
    ) {
        Request request = new Request(id, checkForDanglingTasks, timeout);
        request.setPageParams(pageParams);
        request.setAllowNoResources(allowNoResources);
        return request;
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request mutateInstanceForVersion(Request instance, TransportVersion version) {
        return version.supports(DANGLING_TASKS) ? instance : new Request(instance.getId(), true, TimeValue.MAX_VALUE);
    }
}
