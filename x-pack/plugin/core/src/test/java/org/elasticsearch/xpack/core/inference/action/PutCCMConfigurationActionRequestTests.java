/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class PutCCMConfigurationActionRequestTests extends AbstractBWCWireSerializationTestCase<PutCCMConfigurationAction.Request> {

    public void testValidate_BothFieldsNull_ReturnsException() {
        var req = new PutCCMConfigurationAction.Request(null, null, randomTimeValue(), randomTimeValue());
        var ex = req.validate();
        assertNotNull(ex);
        assertThat(ex.getMessage(), containsString("At least one of [api_key] or [enabled] must be provided"));
    }

    public void testValidate_BothFieldsSet_ReturnsException() {
        var req = new PutCCMConfigurationAction.Request(
            new SecureString("key".toCharArray()), Boolean.TRUE, randomTimeValue(), randomTimeValue());
        var ex = req.validate();
        assertNotNull(ex);
        assertThat(ex.getMessage(), containsString("Only one of [api_key] or [enabled] can be provided but not both"));
    }

    public void testValidate_EnabledTrue_ReturnsException() {
        var req = new PutCCMConfigurationAction.Request(null, Boolean.TRUE, randomTimeValue(), randomTimeValue());
        var ex = req.validate();
        assertNotNull(ex);
        assertThat(ex.getMessage(), containsString("The [enabled] field must be set to [false] when disabling CCM"));
    }

    public void testValidate_ApiKeyEmpty() {
        var req = new PutCCMConfigurationAction.Request(
            new SecureString("".toCharArray()), null, randomTimeValue(), randomTimeValue());
        var ex = req.validate();
        assertNotNull(ex);
        assertThat(ex.getMessage(), containsString("The [api_key] field cannot be an empty string"));
    }

    public void testValidate_ApiKeyValid_DoesNotReturnAnException() {
        var req = new PutCCMConfigurationAction.Request(
            new SecureString("validkey".toCharArray()), null, randomTimeValue(), randomTimeValue());
        var ex = req.validate();
        assertNull(ex);
    }

    public void testValidate_EnabledFalse_DoesNotReturnAnException() {
        var req = new PutCCMConfigurationAction.Request(null, Boolean.FALSE, randomTimeValue(), randomTimeValue());
        var ex = req.validate();
        assertNull(ex);
    }

    @Override
    protected PutCCMConfigurationAction.Request mutateInstanceForVersion(
        PutCCMConfigurationAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }

    @Override
    protected Writeable.Reader<PutCCMConfigurationAction.Request> instanceReader() {
        return PutCCMConfigurationAction.Request::new;
    }

    @Override
    protected PutCCMConfigurationAction.Request createTestInstance() {
        return new PutCCMConfigurationAction.Request(
            new SecureString(randomAlphaOfLength(10).toCharArray()),
            randomOptionalBoolean(),
            randomTimeValue(),
            randomTimeValue()
        );
    }

    @Override
    protected PutCCMConfigurationAction.Request mutateInstance(PutCCMConfigurationAction.Request instance) throws IOException {
        var apiKey = instance.getApiKey();
        var enabled = instance.getEnabled();
        var masterNodeTimeout = instance.masterNodeTimeout();
        var ackTimeout = instance.ackTimeout();

        return switch (randomIntBetween(0, 2)) {
            case 0 -> {
                var newApiKey = apiKey == null ? new SecureString(randomAlphaOfLength(12).toCharArray()) : null;
                yield new PutCCMConfigurationAction.Request(newApiKey, enabled, masterNodeTimeout, ackTimeout);
            }
            case 1 -> {
                var newEnabled = (enabled == null) ? randomBoolean() : null;
                yield new PutCCMConfigurationAction.Request(apiKey, newEnabled, masterNodeTimeout, ackTimeout);
            }
            case 2 -> new PutCCMConfigurationAction.Request(null, null, masterNodeTimeout, ackTimeout);
            default -> throw new IllegalStateException("Unexpected value");
        };
    }
}
