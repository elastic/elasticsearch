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
import org.elasticsearch.xpack.core.transform.action.StartTransformAction.Request;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskParams;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;

import static java.time.Instant.ofEpochMilli;
import static org.elasticsearch.xpack.core.transform.transforms.TransformConfig.TRANSFORM_CLOUD_CREDENTIAL_ON_REQUEST;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class StartTransformActionRequestTests extends AbstractWireSerializingTransformTestCase<Request> {

    private static final TransportVersion TRANSFORM_START_INITIAL_DELAY = TransportVersion.fromName("transform_start_initial_delay");

    @Override
    protected Request createTestInstance() {
        Request request = new Request(
            randomAlphaOfLengthBetween(1, 20),
            randomBoolean() ? ofEpochMilli(randomNonNegativeLong()) : null,
            randomBoolean() ? randomTimeValue() : null,
            randomTimeValue()
        );
        // Randomly include a cloud credential so the wire path with the optional field is exercised
        // by the inherited round-trip test even though the field is excluded from equals/hashCode.
        request.setCloudCredential(randomBoolean() ? randomCloudCredential() : null);
        return request;
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request mutateInstance(Request instance) {
        String id = instance.getId();
        Instant from = instance.from();
        TimeValue initialDelay = instance.getInitialDelay();
        TimeValue timeout = instance.ackTimeout();

        switch (between(0, 3)) {
            case 0 -> id += randomAlphaOfLengthBetween(1, 5);
            case 1 -> from = from != null ? from.plus(Duration.ofDays(1)) : Instant.ofEpochMilli(randomNonNegativeLong());
            case 2 -> timeout = new TimeValue(timeout.duration() + randomLongBetween(1, 5), timeout.timeUnit());
            case 3 -> initialDelay = initialDelay != null
                ? new TimeValue(initialDelay.duration() + randomLongBetween(1, 5), initialDelay.timeUnit())
                : randomTimeValue();
            default -> throw new AssertionError("Illegal randomization branch");
        }

        Request mutated = new Request(id, from, initialDelay, timeout);
        mutated.setCloudCredential(instance.getCloudCredential());
        return mutated;
    }

    @Override
    protected Request mutateInstanceForVersion(Request instance, TransportVersion version) {
        // cloudCredential is excluded from Request.equals so it passes through unchanged here; the explicit
        // drop semantics are asserted by testCloudCredentialDroppedWhenWireVersionTooOld. initialDelay is part
        // of equals, so it must round-trip as null for versions that predate the initial_delay parameter.
        TimeValue initialDelay = version.supports(TRANSFORM_START_INITIAL_DELAY) ? instance.getInitialDelay() : null;
        Request mutated = new Request(instance.getId(), instance.from(), initialDelay, instance.ackTimeout());
        mutated.setCloudCredential(instance.getCloudCredential());
        return mutated;
    }

    @Override
    protected Collection<TransportVersion> bwcVersions() {
        // Requests carrying initial_delay are rejected instead of silently dropping the value on older nodes.
        return super.bwcVersions().stream().filter(version -> version.supports(TRANSFORM_START_INITIAL_DELAY)).toList();
    }

    public void testInitialDelayCannotSerializeToOlderNode() throws IOException {
        testSerializationIsNotBackwardsCompatible(
            TRANSFORM_START_INITIAL_DELAY,
            request -> request.getInitialDelay() != null,
            "Cannot send a _start request with "
                + TransformTaskParams.INITIAL_DELAY.getPreferredName()
                + " to an outdated node. Please upgrade the node to 9.6.0+ and try again."
        );
    }

    public void testCloudCredentialRoundTripPreservesValue() throws IOException {
        String secret = randomAlphaOfLengthBetween(8, 32);
        Request original = new Request(randomAlphaOfLengthBetween(1, 20), null, randomTimeValue());
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
        Request original = new Request(randomAlphaOfLengthBetween(1, 20), null, randomTimeValue());
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
        var request = new Request(randomAlphaOfLengthBetween(1, 20), null, randomTimeValue());
        request.setCloudCredential(credential);

        request.close();
        // SecureString.length() throws once close() has zeroed the underlying char array.
        expectThrows(IllegalStateException.class, () -> credential.value().length());

        // Second close must not throw.
        request.close();
    }

    public void testRequestCloseIsIdempotentWithoutCredential() {
        // Non-UIAM callers leave the credential null. The same close-twice path must be a no-op.
        var request = new Request(randomAlphaOfLengthBetween(1, 20), null, randomTimeValue());

        request.close();
        request.close();
    }

    private static CloudCredential randomCloudCredential() {
        return new CloudCredential(new SecureString(randomAlphaOfLengthBetween(8, 32).toCharArray()));
    }
}
