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
import org.elasticsearch.xpack.core.transform.action.PutTransformAction.Request;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.junit.Before;

import java.io.IOException;

import static org.elasticsearch.xpack.core.transform.transforms.TransformConfig.TRANSFORM_CLOUD_CREDENTIAL_ON_REQUEST;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PutTransformActionRequestTests extends AbstractWireSerializingTransformTestCase<Request> {
    private String transformId;

    @Before
    public void setupTransformId() {
        transformId = randomAlphaOfLengthBetween(1, 10);
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        TransformConfig config = TransformConfigTests.randomTransformConfigWithoutHeaders(transformId);
        Request request = new Request(config, randomBoolean(), randomTimeValue());
        // Randomly include a cloud credential so the wire path with the optional field is exercised
        // by the inherited round-trip test even though the field is excluded from equals/hashCode.
        request.setCloudCredential(randomBoolean() ? randomCloudCredential() : null);
        return request;
    }

    @Override
    protected Request mutateInstance(Request instance) {
        TransformConfig config = instance.getConfig();
        boolean deferValidation = instance.isDeferValidation();
        TimeValue timeout = instance.ackTimeout();

        switch (between(0, 2)) {
            case 0 -> config = new TransformConfig.Builder(config).setId(config.getId() + randomAlphaOfLengthBetween(1, 5)).build();
            case 1 -> deferValidation ^= true;
            case 2 -> timeout = new TimeValue(timeout.duration() + randomLongBetween(1, 5), timeout.timeUnit());
            default -> throw new AssertionError("Illegal randomization branch");
        }

        Request mutated = new Request(config, deferValidation, timeout);
        mutated.setCloudCredential(instance.getCloudCredential());
        return mutated;
    }

    @Override
    protected Request mutateInstanceForVersion(Request instance, TransportVersion version) {
        // SourceConfig has version-gated fields (projectRouting, indicesOptions); delegate to the shared
        // TransformConfig helper that drops them for older versions so the BWC baseline matches the wire round-trip.
        // cloudCredential is excluded from Request.equals so it passes through unchanged here; the explicit
        // drop semantics are asserted by testCloudCredentialDroppedWhenWireVersionTooOld.
        Request mutated = new Request(
            TransformConfigTests.mutateForVersion(instance.getConfig(), version),
            instance.isDeferValidation(),
            instance.ackTimeout()
        );
        mutated.setCloudCredential(instance.getCloudCredential());
        return mutated;
    }

    public void testCloudCredentialRoundTripPreservesValue() throws IOException {
        String secret = randomAlphaOfLengthBetween(8, 32);
        Request original = new Request(
            TransformConfigTests.randomTransformConfigWithoutHeaders(transformId),
            randomBoolean(),
            randomTimeValue()
        );
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
        Request original = new Request(
            TransformConfigTests.randomTransformConfigWithoutHeaders(transformId),
            randomBoolean(),
            randomTimeValue()
        );
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
        var request = new Request(
            TransformConfigTests.randomTransformConfigWithoutHeaders(transformId),
            randomBoolean(),
            randomTimeValue()
        );
        request.setCloudCredential(credential);

        request.close();
        // SecureString.length() throws once close() has zeroed the underlying char array.
        expectThrows(IllegalStateException.class, () -> credential.value().length());

        // Second close must not throw.
        request.close();
    }

    public void testRequestCloseIsIdempotentWithoutCredential() {
        // Non-UIAM callers leave the credential null. The same close-twice path must be a no-op.
        var request = new Request(
            TransformConfigTests.randomTransformConfigWithoutHeaders(transformId),
            randomBoolean(),
            randomTimeValue()
        );

        request.close();
        request.close();
    }

    private static CloudCredential randomCloudCredential() {
        return new CloudCredential(new SecureString(randomAlphaOfLengthBetween(8, 32).toCharArray()));
    }
}
