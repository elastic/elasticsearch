/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.core.security.cloud.CloudCredential;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction.Request;
import org.elasticsearch.xpack.core.transform.transforms.AuthorizationStateTests;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigUpdate;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigUpdateTests;

import java.io.IOException;

import static org.elasticsearch.xpack.core.transform.transforms.TransformConfig.TRANSFORM_CLOUD_CREDENTIAL_ON_REQUEST;
import static org.elasticsearch.xpack.core.transform.transforms.TransformConfigUpdateTests.randomTransformConfigUpdate;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class UpdateTransformActionRequestTests extends AbstractWireSerializingTransformTestCase<Request> {

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomTransformConfigUpdate(), randomAlphaOfLength(10), randomBoolean(), randomTimeValue());
        if (randomBoolean()) {
            request.setConfig(TransformConfigTests.randomTransformConfig());
        }
        if (randomBoolean()) {
            request.setAuthState(AuthorizationStateTests.randomAuthorizationState());
        }
        // Randomly include a cloud credential so the wire path with the optional field is exercised
        // by the inherited round-trip test even though the field is excluded from equals/hashCode.
        if (randomBoolean()) {
            request.setCloudCredential(randomCloudCredential());
        }
        return request;
    }

    @Override
    protected Request mutateInstance(Request instance) {
        String id = instance.getId();
        TransformConfigUpdate update = instance.getUpdate();
        boolean deferValidation = instance.isDeferValidation();
        TimeValue timeout = instance.getTimeout();

        switch (between(0, 3)) {
            case 0 -> id += randomAlphaOfLengthBetween(1, 5);
            case 1 -> {
                String description = update.getDescription() == null ? "" : update.getDescription();
                description += randomAlphaOfLengthBetween(1, 5);
                // fix corner case that description gets too long
                if (description.length() > 1000) {
                    description = description.substring(description.length() - 1000, description.length());
                }
                update = new TransformConfigUpdate(
                    update.getSource(),
                    update.getDestination(),
                    update.getFrequency(),
                    update.getSyncConfig(),
                    description,
                    update.getSettings(),
                    update.getMetadata(),
                    update.getRetentionPolicyConfig()
                );
            }
            case 2 -> deferValidation ^= true;
            case 3 -> timeout = new TimeValue(timeout.duration() + randomLongBetween(1, 5), timeout.timeUnit());
            default -> throw new AssertionError("Illegal randomization branch");
        }

        Request mutated = new Request(update, id, deferValidation, timeout);
        mutated.setCloudCredential(instance.getCloudCredential());
        return mutated;
    }

    @Override
    protected Request mutateInstanceForVersion(Request instance, TransportVersion version) {
        // Both the inner TransformConfigUpdate and the optional TransformConfig carry a SourceConfig with
        // version-gated fields; drop them for older versions so the BWC baseline matches the wire round-trip.
        // cloudCredential is excluded from Request.equals so it passes through unchanged here; the explicit
        // drop semantics are asserted by testCloudCredentialDroppedWhenWireVersionTooOld.
        Request mutated = new Request(
            TransformConfigUpdateTests.mutateForVersion(instance.getUpdate(), version),
            instance.getId(),
            instance.isDeferValidation(),
            instance.getTimeout()
        );
        TransformConfig config = instance.getConfig();
        if (config != null) {
            mutated.setConfig(TransformConfigTests.mutateForVersion(config, version));
        }
        if (instance.getAuthState() != null) {
            mutated.setAuthState(instance.getAuthState());
        }
        mutated.setCloudCredential(instance.getCloudCredential());
        return mutated;
    }

    public void testCloudCredentialRoundTripPreservesValue() throws IOException {
        String secret = randomAlphaOfLengthBetween(8, 32);
        Request original = new Request(randomTransformConfigUpdate(), randomAlphaOfLength(10), randomBoolean(), randomTimeValue());
        original.setCloudCredential(new CloudCredential(new SecureString(secret.toCharArray())));

        Request copy = copyWriteable(original, getNamedWriteableRegistry(), instanceReader());
        try {
            assertThat(copy.getCloudCredential(), is(notNullValue()));
            assertThat(copy.getCloudCredential().value().toString(), is(secret));
        } finally {
            copy.close();
        }
    }

    public void testCloudCredentialDroppedWhenWireVersionTooOld() throws IOException {
        Request original = new Request(randomTransformConfigUpdate(), randomAlphaOfLength(10), randomBoolean(), randomTimeValue());
        original.setCloudCredential(randomCloudCredential());

        var olderVersion = TransportVersionUtils.randomVersionNotSupporting(TRANSFORM_CLOUD_CREDENTIAL_ON_REQUEST);
        Request copy = copyWriteable(original, getNamedWriteableRegistry(), instanceReader(), olderVersion);
        try {
            // Older receivers can't decode the new optional field, so it must round-trip as null.
            assertThat(copy.getCloudCredential(), is(nullValue()));
        } finally {
            copy.close();
        }
    }

    public void testRequestCloseIsIdempotentWithCredential() {
        // Both the sender's and receiver's listeners may fire close() on the same Request instance
        // (local dispatch reuses the same instance). The contract we rely on is that a second close()
        // is a safe no-op so we never need to coordinate which side closes the credential.
        var credential = randomCloudCredential();
        var request = new Request(randomTransformConfigUpdate(), randomAlphaOfLength(10), randomBoolean(), randomTimeValue());
        request.setCloudCredential(credential);

        request.close();
        // SecureString.length() throws once close() has zeroed the underlying char array.
        expectThrows(IllegalStateException.class, () -> credential.value().length());

        // Second close must not throw.
        request.close();
    }

    public void testRequestCloseIsIdempotentWithoutCredential() {
        // Non-UIAM callers leave the credential null. The same close-twice path must be a no-op.
        var request = new Request(randomTransformConfigUpdate(), randomAlphaOfLength(10), randomBoolean(), randomTimeValue());

        request.close();
        request.close();
    }

    private static CloudCredential randomCloudCredential() {
        return new CloudCredential(new SecureString(randomAlphaOfLengthBetween(8, 32).toCharArray()));
    }
}
