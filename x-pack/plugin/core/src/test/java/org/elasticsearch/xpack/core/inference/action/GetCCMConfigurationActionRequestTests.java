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

public class GetCCMConfigurationActionRequestTests extends AbstractBWCWireSerializationTestCase<GetCCMConfigurationAction.Request> {

    @Override
    protected GetCCMConfigurationAction.Request mutateInstanceForVersion(
        GetCCMConfigurationAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }

    @Override
    protected Writeable.Reader<GetCCMConfigurationAction.Request> instanceReader() {
        return GetCCMConfigurationAction.Request::new;
    }

    @Override
    protected GetCCMConfigurationAction.Request createTestInstance() {
        return new GetCCMConfigurationAction.Request();
    }

    @Override
    protected GetCCMConfigurationAction.Request mutateInstance(GetCCMConfigurationAction.Request instance) throws IOException {
        // need to return null here because the instances will always be identical
        return null;
    }
}
