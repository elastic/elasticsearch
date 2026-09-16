/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.InferenceServiceConfiguration.Builder;
import org.elasticsearch.inference.configuration.InferenceServiceFeatures;
import org.elasticsearch.inference.configuration.InferenceServiceFeaturesTests;
import org.elasticsearch.inference.configuration.NonStreamingChatFeature;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.inference.InferenceServiceConfigurationTestUtils.getRandomServiceConfiguration;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class InferenceServiceConfigurationTests extends AbstractBWCSerializationTestCase<InferenceServiceConfiguration> {

    @Override
    protected InferenceServiceConfiguration createTestInstance() {
        return InferenceServiceConfigurationTestUtils.getRandomServiceConfigurationField();
    }

    @Override
    protected InferenceServiceConfiguration doParseInstance(XContentParser parser) throws IOException {
        return InferenceServiceConfiguration.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // A random key inserted directly into the configurations map would not parse as a
        // SettingsConfiguration. Unknown fields inside each entry are fine because the parser
        // is lenient (supportsUnknownFields = true). Likewise every key in the features object is
        // a feature name resolved through the registry, so a random key there is an unknown feature.
        return field -> field.startsWith("configurations") || field.equals("features");
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return InferenceServiceFeatures.NAMED_X_CONTENT_REGISTRY;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(InferenceServiceFeatures.getNamedWriteables());
    }

    @Override
    protected Writeable.Reader<InferenceServiceConfiguration> instanceReader() {
        return InferenceServiceConfiguration::new;
    }

    @Override
    protected InferenceServiceConfiguration mutateInstance(InferenceServiceConfiguration instance) {
        var service = instance.getService();
        var name = instance.getName();
        var taskTypes = instance.getTaskTypes();
        var configurations = instance.getConfigurations();
        var features = instance.getFeatures();

        return switch (randomInt(4)) {
            case 0 -> new Builder().setService(randomValueOtherThan(service, () -> randomAlphaOfLength(10)))
                .setName(name)
                .setTaskTypes(taskTypes)
                .setConfigurations(configurations)
                .setFeatures(features)
                .build();
            case 1 -> new Builder().setService(service)
                .setName(randomValueOtherThan(name, () -> randomAlphaOfLength(6)))
                .setTaskTypes(taskTypes)
                .setConfigurations(configurations)
                .setFeatures(features)
                .build();
            case 2 -> new Builder().setService(service)
                .setName(name)
                .setTaskTypes(randomValueOtherThan(taskTypes, InferenceServiceConfigurationTestUtils::getRandomTaskTypes))
                .setConfigurations(configurations)
                .setFeatures(features)
                .build();
            case 3 -> new Builder().setService(service)
                .setName(name)
                .setTaskTypes(taskTypes)
                .setConfigurations(randomValueOtherThan(configurations, () -> getRandomServiceConfiguration(5)))
                .setFeatures(features)
                .build();
            case 4 -> new Builder().setService(service)
                .setName(name)
                .setTaskTypes(taskTypes)
                .setConfigurations(configurations)
                .setFeatures(randomValueOtherThan(features, InferenceServiceFeaturesTests::randomInstance))
                .build();
            default -> throw new AssertionError("unexpected");
        };
    }

    @Override
    protected InferenceServiceConfiguration mutateInstanceForVersion(InferenceServiceConfiguration instance, TransportVersion version) {
        return instance;
    }

    public void testToXContent() throws IOException {
        var content = XContentHelper.stripWhitespace("""
            {
               "service": "some_provider",
               "name": "Some Provider",
               "task_types": ["text_embedding", "completion"],
               "configurations": {
                    "text_field_configuration": {
                        "description": "Wow, this tooltip is useful.",
                        "label": "Very important field",
                        "required": true,
                        "sensitive": true,
                        "updatable": false,
                        "type": "str",
                        "supported_task_types": ["text_embedding", "completion"]
                    },
                    "numeric_field_configuration": {
                        "default_value": 3,
                        "description": "Wow, this tooltip is useful.",
                        "label": "Very important numeric field",
                        "required": true,
                        "sensitive": false,
                        "updatable": true,
                        "type": "int",
                        "supported_task_types": ["text_embedding", "completion"]
                    }
               }
            }
            """);

        var configuration = InferenceServiceConfiguration.fromXContentBytes(new BytesArray(content), XContentType.JSON);
        boolean humanReadable = true;
        var originalBytes = toShuffledXContent(configuration, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        InferenceServiceConfiguration parsed;
        try (var parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = InferenceServiceConfiguration.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    public void testToXContent_EmptyTaskTypes() throws IOException {
        var content = XContentHelper.stripWhitespace("""
            {
               "service": "some_provider",
               "name": "Some Provider",
               "task_types": [],
               "configurations": {
                    "text_field_configuration": {
                        "description": "Wow, this tooltip is useful.",
                        "label": "Very important field",
                        "required": true,
                        "sensitive": true,
                        "updatable": false,
                        "type": "str",
                        "supported_task_types": ["text_embedding", "completion"]
                    },
                    "numeric_field_configuration": {
                        "default_value": 3,
                        "description": "Wow, this tooltip is useful.",
                        "label": "Very important numeric field",
                        "required": true,
                        "sensitive": false,
                        "updatable": true,
                        "type": "int",
                        "supported_task_types": ["text_embedding", "completion"]
                    }
               }
            }
            """);

        var configuration = InferenceServiceConfiguration.fromXContentBytes(new BytesArray(content), XContentType.JSON);
        boolean humanReadable = true;
        var originalBytes = toShuffledXContent(configuration, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        InferenceServiceConfiguration parsed;
        try (var parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = InferenceServiceConfiguration.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    public void testToXContent_WithFeatures() throws IOException {
        var content = XContentHelper.stripWhitespace("""
            {
               "service": "openai",
               "name": "OpenAI",
               "task_types": ["completion"],
               "configurations": {},
               "features": {"non_streaming_chat": {"supported": true}}
            }
            """);

        var configuration = InferenceServiceConfiguration.fromXContentBytes(new BytesArray(content), XContentType.JSON);
        assertThat(configuration.getFeatures(), is(InferenceServiceFeatures.of(NonStreamingChatFeature.SUPPORTED_INSTANCE)));
        boolean humanReadable = true;
        var originalBytes = toShuffledXContent(configuration, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        InferenceServiceConfiguration parsed;
        try (var parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = InferenceServiceConfiguration.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    public void testToXContent_OmitsFeaturesWhenNull() throws IOException {
        var configuration = new Builder().setService("s").setName("n").build();
        boolean humanReadable = true;
        BytesReference bytes = toXContent(configuration, XContentType.JSON, humanReadable);
        assertThat(bytes.utf8ToString(), not(containsString("features")));
    }

    public void testFromXContent_FeaturesIsOptional() throws IOException {
        var content = XContentHelper.stripWhitespace("""
            {
               "service": "some_provider",
               "name": "Some Provider",
               "task_types": [],
               "configurations": {}
            }
            """);
        var configuration = InferenceServiceConfiguration.fromXContentBytes(new BytesArray(content), XContentType.JSON);
        assertNull(configuration.getFeatures());
    }

    public void testFromXContent_FeaturesWithNonStreamingChatUnsupported() throws IOException {
        var content = XContentHelper.stripWhitespace("""
            {
               "service": "openai",
               "name": "OpenAI",
               "task_types": ["completion"],
               "configurations": {},
               "features": {"non_streaming_chat": {"supported": false}}
            }
            """);
        var configuration = InferenceServiceConfiguration.fromXContentBytes(new BytesArray(content), XContentType.JSON);
        assertThat(configuration.getFeatures(), is(InferenceServiceFeatures.of(NonStreamingChatFeature.UNSUPPORTED_INSTANCE)));
    }

    public void testFromXContent_NestedFeatureParseFailureIsWrappedByOuterParser() throws IOException {
        var content = XContentHelper.stripWhitespace("""
            {
               "service": "openai",
               "name": "OpenAI",
               "task_types": ["completion"],
               "configurations": {},
               "features": {"non_streaming_chat": {}}
            }
            """);

        var exception = expectThrows(
            XContentParseException.class,
            () -> InferenceServiceConfiguration.fromXContentBytes(new BytesArray(content), XContentType.JSON)
        );
        assertThat(exception.getMessage(), containsString("[features]"));
    }

    public void testFromXContent_ThrowsWhenTheFeatureNameIsUnknown() throws IOException {
        var content = XContentHelper.stripWhitespace("""
            {
               "service": "openai",
               "name": "OpenAI",
               "task_types": ["completion"],
               "configurations": {},
               "features": {"not_a_real_feature": {"supported": true}}
            }
            """);

        var exception = expectThrows(
            XContentParseException.class,
            () -> InferenceServiceConfiguration.fromXContentBytes(new BytesArray(content), XContentType.JSON)
        );
        assertThat(exception.getMessage(), containsString("[features]"));
    }

    public void testFromXContent_ParsesConfigurationsAsSettingsConfiguration() throws IOException {
        var content = XContentHelper.stripWhitespace("""
            {
              "service": "some_provider",
              "name": "Some Provider",
              "task_types": ["text_embedding"],
              "configurations": {
                "api_key": {
                  "description": "The API key.",
                  "label": "API Key",
                  "required": true,
                  "sensitive": true,
                  "updatable": true,
                  "type": "str",
                  "supported_task_types": ["text_embedding"]
                }
              }
            }
            """);

        var configuration = InferenceServiceConfiguration.fromXContentBytes(new BytesArray(content), XContentType.JSON);

        var apiKey = configuration.getConfigurations().get("api_key");
        assertThat(apiKey.getLabel(), is("API Key"));
        assertTrue(apiKey.isRequired());
        assertThat(apiKey.getType(), is(SettingsConfigurationFieldType.STRING));
        assertThat(apiKey.getSupportedTaskTypes(), is(EnumSet.of(TaskType.TEXT_EMBEDDING)));
    }

    public void testBuild_ThrowsWhenServiceIsNull() {
        expectThrows(NullPointerException.class, () -> new Builder().setName("n").build());
    }

    public void testBuild_ThrowsWhenNameIsNull() {
        expectThrows(NullPointerException.class, () -> new Builder().setService("s").build());
    }

    public void testBuild_ThrowsWhenConfigurationsIsNull() {
        expectThrows(NullPointerException.class, () -> new Builder().setService("s").setName("n").setConfigurations(null).build());
    }

    public void testBuild_SucceedsWithNullFeatures() {
        var configuration = new Builder().setService("s").setName("n").setFeatures(null).build();
        assertNull(configuration.getFeatures());
    }

    public void testBuilderDefaults_EmptyTaskTypesAndConfigurations() {
        var configuration = new Builder().setService("s").setName("n").build();
        assertThat(configuration.getTaskTypes(), is(java.util.EnumSet.noneOf(TaskType.class)));
        assertThat(configuration.getConfigurations(), is(anEmptyMap()));
    }

    public void testGetConfigurations_ReturnsDefensiveCopy() {
        var configuration = InferenceServiceConfigurationTestUtils.getRandomServiceConfigurationField();
        assertNotSame(configuration.getConfigurations(), configuration.getConfigurations());
    }

    public void testGetConfigurations_MutatingReturnedMapDoesNotAffectInstance() {
        var configuration = new Builder().setService("s").setName("n").setConfigurations(getRandomServiceConfiguration(1)).build();
        // ensure at least one entry
        if (configuration.getConfigurations().isEmpty()) {
            configuration = new Builder().setService("s")
                .setName("n")
                .setConfigurations(Map.of("key", SettingsConfigurationTestUtils.getRandomSettingsConfigurationField()))
                .build();
        }
        var returned = configuration.getConfigurations();
        var originalSize = returned.size();
        returned.clear();
        assertThat(configuration.getConfigurations().size(), is(originalSize));
    }
}
