/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.regionpolicy.RegionPolicyTests;

import java.io.IOException;

public class PutRegionPolicyActionRequestTests extends AbstractBWCSerializationTestCase<PutRegionPolicyAction.Request> {

    @Override
    protected PutRegionPolicyAction.Request mutateInstanceForVersion(PutRegionPolicyAction.Request instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected Writeable.Reader<PutRegionPolicyAction.Request> instanceReader() {
        return PutRegionPolicyAction.Request::new;
    }

    @Override
    protected PutRegionPolicyAction.Request createTestInstance() {
        return new PutRegionPolicyAction.Request(RegionPolicyTests.createRandom());
    }

    @Override
    protected PutRegionPolicyAction.Request mutateInstance(PutRegionPolicyAction.Request instance) throws IOException {
        return new PutRegionPolicyAction.Request(randomValueOtherThan(instance.regionPolicy(), RegionPolicyTests::createRandom));
    }

    @Override
    protected PutRegionPolicyAction.Request doParseInstance(XContentParser parser) throws IOException {
        return PutRegionPolicyAction.Request.parseRequest(parser);
    }
}
