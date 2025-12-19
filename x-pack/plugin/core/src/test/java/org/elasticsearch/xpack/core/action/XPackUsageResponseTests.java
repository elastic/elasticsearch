/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.XPackFeatureUsage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class XPackUsageResponseTests extends AbstractWireSerializingTestCase<XPackUsageResponse> {

    private static final String TEST_FEATURE_USAGE = "test-feature-usage";

    @Override
    protected Writeable.Reader<XPackUsageResponse> instanceReader() {
        return XPackUsageResponse::new;
    }

    @Override
    protected XPackUsageResponse createTestInstance() {
        return new XPackUsageResponse(List.of(new TestXpackFeatureUsage(TEST_FEATURE_USAGE, randomBoolean(), randomBoolean())));
    }

    @Override
    protected XPackUsageResponse mutateInstance(XPackUsageResponse instance) throws IOException {
        List<XPackFeatureUsage> usages = new ArrayList<>(instance.getUsages());
        usages.add(new TestXpackFeatureUsage(TEST_FEATURE_USAGE, randomBoolean(), randomBoolean()));

        return new XPackUsageResponse(usages);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = List.of(
            new NamedWriteableRegistry.Entry(XPackFeatureUsage.class, TEST_FEATURE_USAGE, TestXpackFeatureUsage::new)
        );

        return new NamedWriteableRegistry(entries);
    }

    private static class TestXpackFeatureUsage extends XPackFeatureUsage {
        TestXpackFeatureUsage(StreamInput input) throws IOException {
            super(input);
        }

        TestXpackFeatureUsage(String name, boolean available, boolean enabled) {
            super(name, available, enabled);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.minimumCompatible();
        }
    }
}
