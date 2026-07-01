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
import org.elasticsearch.xpack.core.inference.regionpolicy.RegionPolicyDoc;
import org.elasticsearch.xpack.core.inference.regionpolicy.RegionPolicyDocTests;

import java.io.IOException;

public class RegionPolicyResponseTests extends AbstractBWCSerializationTestCase<RegionPolicyResponse> {

    @Override
    protected RegionPolicyResponse mutateInstanceForVersion(RegionPolicyResponse instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected Writeable.Reader<RegionPolicyResponse> instanceReader() {
        return RegionPolicyResponse::new;
    }

    @Override
    protected RegionPolicyResponse createTestInstance() {
        return new RegionPolicyResponse(createRandomDoc());
    }

    @Override
    protected RegionPolicyResponse mutateInstance(RegionPolicyResponse instance) throws IOException {
        return new RegionPolicyResponse(randomValueOtherThan(instance.regionPolicy(), this::createRandomDoc));
    }

    @Override
    protected RegionPolicyResponse doParseInstance(XContentParser parser) throws IOException {
        return new RegionPolicyResponse(RegionPolicyDoc.STRICT_PARSER.apply(parser, null));
    }

    private RegionPolicyDoc createRandomDoc() {
        return RegionPolicyDocTests.createRandom(this::randomInstant);
    }
}
