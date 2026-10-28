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

public class DeleteCCMConfigurationActionRequestTests extends AbstractBWCWireSerializationTestCase<DeleteCCMConfigurationAction.Request> {

    @Override
    protected DeleteCCMConfigurationAction.Request mutateInstanceForVersion(
        DeleteCCMConfigurationAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }

    @Override
    protected Writeable.Reader<DeleteCCMConfigurationAction.Request> instanceReader() {
        return DeleteCCMConfigurationAction.Request::new;
    }

    @Override
    protected DeleteCCMConfigurationAction.Request createTestInstance() {
        return new DeleteCCMConfigurationAction.Request(randomTimeValue(), randomTimeValue());
    }

    @Override
    protected DeleteCCMConfigurationAction.Request mutateInstance(DeleteCCMConfigurationAction.Request instance) throws IOException {
        // need to return null here because the instances will always be identical
        return null;
    }
}
