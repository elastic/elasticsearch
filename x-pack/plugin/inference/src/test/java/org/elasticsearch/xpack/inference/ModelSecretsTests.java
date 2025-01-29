/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ModelSecretsTests extends AbstractWireSerializingTestCase<ModelSecrets> {

    public static ModelSecrets createRandomInstance() {
        return new ModelSecrets(randomSecretSettings());
    }

    private static SecretSettings randomSecretSettings() {
        return new FakeSecretSettings(randomAlphaOfLengthBetween(8, 10));
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(new NamedWriteableRegistry.Entry(SecretSettings.class, FakeSecretSettings.NAME, FakeSecretSettings::new))
        );
    }

    @Override
    protected Writeable.Reader<ModelSecrets> instanceReader() {
        return ModelSecrets::new;
    }

    @Override
    protected ModelSecrets createTestInstance() {
        return createRandomInstance();
    }

    @Override
    protected ModelSecrets mutateInstance(ModelSecrets instance) {
        return randomValueOtherThan(instance, ModelSecretsTests::createRandomInstance);
    }

    public record FakeSecretSettings(String apiKey) implements SecretSettings {
        public static final String API_KEY = "api_key";
        public static final String NAME = "fake_secret_settings";

        FakeSecretSettings(StreamInput in) throws IOException {
            this(in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(apiKey);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(API_KEY, apiKey);
            builder.endObject();
            return builder;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.V_8_11_X;
        }

        @Override
        public SecretSettings newSecretSettings(Map<String, Object> newSecrets) {
            return new FakeSecretSettings(newSecrets.get(API_KEY).toString());
        }
    }
}
