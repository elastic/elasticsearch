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

public class CCMEnabledActionResponseTests extends AbstractBWCWireSerializationTestCase<CCMEnabledActionResponse> {

    public void testIsEnabled() {
        var responseEnabled = new CCMEnabledActionResponse(true);
        assertTrue(responseEnabled.isEnabled());

        var responseDisabled = new CCMEnabledActionResponse(false);
        assertFalse(responseDisabled.isEnabled());
    }

    @Override
    protected CCMEnabledActionResponse mutateInstanceForVersion(CCMEnabledActionResponse instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected Writeable.Reader<CCMEnabledActionResponse> instanceReader() {
        return CCMEnabledActionResponse::new;
    }

    @Override
    protected CCMEnabledActionResponse createTestInstance() {
        return new CCMEnabledActionResponse(randomBoolean());
    }

    @Override
    protected CCMEnabledActionResponse mutateInstance(CCMEnabledActionResponse instance) throws IOException {
        return new CCMEnabledActionResponse(instance.isEnabled() == false);
    }
}
