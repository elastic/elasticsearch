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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

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
            new CloudCredential(new SecureString(randomAlphaOfLengthBetween(16, 256).toCharArray()))
        );
    }

    @Override
    protected PersistedCloudCredential mutateInstance(PersistedCloudCredential instance) {
        if (randomBoolean()) {
            return new PersistedCloudCredential(
                randomValueOtherThan(instance.id(), () -> randomAlphaOfLengthBetween(5, 22)),
                new CloudCredential(new SecureString(instance.credential().value().toString().toCharArray()))
            );
        } else {
            return new PersistedCloudCredential(
                instance.id(),
                new CloudCredential(
                    new SecureString(
                        randomValueOtherThan(instance.credential().value().toString(), () -> randomAlphaOfLengthBetween(16, 256))
                            .toCharArray()
                    )
                )
            );
        }
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testConstructorRejectsNullArguments() {
        var credential = new CloudCredential(new SecureString("v".toCharArray()));
        var npe = expectThrows(NullPointerException.class, () -> new PersistedCloudCredential(null, credential));
        assertThat(npe.getMessage(), containsString("id"));

        var npe2 = expectThrows(NullPointerException.class, () -> new PersistedCloudCredential("an-id", null));
        assertThat(npe2.getMessage(), containsString("credential"));
    }

    public void testToStringRedactsCredential() {
        var instance = createTestInstance();
        var str = instance.toString();
        assertThat(str, containsString(instance.id()));
        assertThat(str, containsString("::es_redacted::"));
        assertThat(str, not(containsString(instance.credential().value().toString())));
    }

    public void testEqualityAndHashCodeBasedOnIdAndCredential() {
        var id = randomAlphaOfLengthBetween(5, 22);
        var chars = randomAlphaOfLengthBetween(16, 64).toCharArray();
        var first = new PersistedCloudCredential(id, new CloudCredential(new SecureString(chars.clone())));
        var second = new PersistedCloudCredential(id, new CloudCredential(new SecureString(chars.clone())));

        assertThat(first, is(equalTo(second)));
        assertThat(first.hashCode(), is(equalTo(second.hashCode())));

        var differentId = new PersistedCloudCredential(
            randomValueOtherThan(id, () -> randomAlphaOfLengthBetween(5, 22)),
            new CloudCredential(new SecureString(chars.clone()))
        );
        assertThat(first, is(not(equalTo(differentId))));
    }
}
