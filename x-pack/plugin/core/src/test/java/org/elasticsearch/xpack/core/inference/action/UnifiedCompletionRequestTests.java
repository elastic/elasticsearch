/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.UnifiedCompletionRequestBody;
import org.elasticsearch.inference.completion.UnifiedCompletionUtils;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.Collection;

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CHAT_COMPLETION_TEMPLATE_ADDED;
import static org.hamcrest.Matchers.is;

public class UnifiedCompletionRequestTests extends AbstractBWCWireSerializationTestCase<UnifiedCompletionRequest> {

    public static UnifiedCompletionRequest createRandom() {
        return new UnifiedCompletionRequest(UnifiedCompletionRequestBodyTests.randomUnifiedCompletionRequest(), randomBoolean());
    }

    public void testStreaming_SetsStreamToTrue() {
        var body = UnifiedCompletionRequestBodyTests.randomUnifiedCompletionRequest();
        var request = UnifiedCompletionRequest.streaming(body);

        assertTrue(request.stream());
        assertThat(request.body(), is(body));
    }

    public void testThrows_WhenBodyIsNull() {
        expectThrows(NullPointerException.class, () -> new UnifiedCompletionRequest(null, randomBoolean()));
    }

    /**
     * Versions before {@link UnifiedCompletionUtils#MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED} refuse to serialize non-text content
     * and versions before {@link UnifiedCompletionUtils#CHAT_COMPLETION_TEMPLATE_ADDED} refuse to serialize a template, so the
     * generic BWC test only covers versions that accept every random instance. The refusals and the older versions are covered by
     * {@link UnifiedCompletionRequestBodyTests#testUnsupportedFieldsAreNotBackwardsCompatible}.
     */
    @Override
    protected Collection<TransportVersion> bwcVersions() {
        return super.bwcVersions().stream().filter(version -> version.supports(CHAT_COMPLETION_TEMPLATE_ADDED)).toList();
    }

    /**
     * {@code stream} is written unconditionally, so only the body needs adjusting for older versions.
     */
    @Override
    protected UnifiedCompletionRequest mutateInstanceForVersion(UnifiedCompletionRequest instance, TransportVersion version) {
        return new UnifiedCompletionRequest(
            UnifiedCompletionRequestBodyTests.mutateInstanceForTransportVersion(instance.body(), version),
            instance.stream()
        );
    }

    @Override
    protected Writeable.Reader<UnifiedCompletionRequest> instanceReader() {
        return UnifiedCompletionRequest::new;
    }

    @Override
    protected UnifiedCompletionRequest createTestInstance() {
        return createRandom();
    }

    @Override
    protected UnifiedCompletionRequest mutateInstance(UnifiedCompletionRequest instance) throws IOException {
        UnifiedCompletionRequestBody body = instance.body();
        boolean stream = instance.stream();
        switch (between(0, 1)) {
            case 0 -> body = randomValueOtherThan(body, UnifiedCompletionRequestBodyTests::randomUnifiedCompletionRequest);
            case 1 -> stream = stream == false;
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new UnifiedCompletionRequest(body, stream);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(UnifiedCompletionRequestBody.getNamedWriteables());
    }
}
