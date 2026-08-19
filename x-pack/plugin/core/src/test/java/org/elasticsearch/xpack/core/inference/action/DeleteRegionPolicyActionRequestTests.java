/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

public class DeleteRegionPolicyActionRequestTests extends AbstractBWCWireSerializationTestCase<DeleteRegionPolicyAction.Request> {

    @Override
    protected DeleteRegionPolicyAction.Request mutateInstanceForVersion(
        DeleteRegionPolicyAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }

    @Override
    protected Writeable.Reader<DeleteRegionPolicyAction.Request> instanceReader() {
        return DeleteRegionPolicyAction.Request::new;
    }

    @Override
    protected DeleteRegionPolicyAction.Request createTestInstance() {
        return new DeleteRegionPolicyAction.Request();
    }

    @Override
    protected DeleteRegionPolicyAction.Request mutateInstance(DeleteRegionPolicyAction.Request instance) throws IOException {
        // need to return null here because the instances will always be identical
        return null;
    }
}
