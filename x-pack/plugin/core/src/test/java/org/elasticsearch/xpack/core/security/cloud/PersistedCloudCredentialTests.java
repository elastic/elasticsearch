/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.cloud;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class PersistedCloudCredentialTests extends AbstractXContentSerializingTestCase<PersistedCloudCredential> {

    @Override
    protected PersistedCloudCredential doParseInstance(XContentParser parser) throws IOException {
        return PersistedCloudCredential.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<PersistedCloudCredential> instanceReader() {
        return PersistedCloudCredential::new;
    }

    @Override
    protected PersistedCloudCredential createTestInstance() {
        return new PersistedCloudCredential(
            randomAlphaOfLengthBetween(5, 22),
            new SecureString(randomAlphaOfLengthBetween(16, 256).toCharArray())
        );
    }

    @Override
    protected PersistedCloudCredential mutateInstance(PersistedCloudCredential instance) {
        if (randomBoolean()) {
            return new PersistedCloudCredential(
                randomValueOtherThan(instance.id(), () -> randomAlphaOfLengthBetween(5, 22)),
                new SecureString(instance.internalApiKey().toString().toCharArray())
            );
        } else {
            return new PersistedCloudCredential(
                instance.id(),
                new SecureString(
                    randomValueOtherThan(instance.internalApiKey().toString(), () -> randomAlphaOfLengthBetween(16, 256)).toCharArray()
                )
            );
        }
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testNewInstanceStampsCurrentVersion() {
        var instance = createTestInstance();
        assertThat(instance.version(), is(equalTo(PersistedCloudCredential.CURRENT_VERSION)));
    }
}
