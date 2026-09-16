/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.configuration;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class InferenceServiceFeaturesTests extends AbstractBWCSerializationTestCase<InferenceServiceFeatures> {

    private static final String TEST_FEATURE_DETAIL = "some detail";

    public static InferenceServiceFeatures randomInstance() {
        return randomBoolean() ? InferenceServiceFeatures.of() : InferenceServiceFeatures.of(NonStreamingChatFeature.of(randomBoolean()));
    }

    @Override
    protected InferenceServiceFeatures createTestInstance() {
        return randomInstance();
    }

    @Override
    protected InferenceServiceFeatures doParseInstance(XContentParser parser) throws IOException {
        return InferenceServiceFeatures.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // Every field of the root object is a feature name resolved through the registry, so an injected random field
        // there is an unknown feature rather than an unknown field to ignore. Insertions inside a feature are left
        // alone because those must stay lenient.
        return String::isEmpty;
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
    protected Writeable.Reader<InferenceServiceFeatures> instanceReader() {
        return InferenceServiceFeatures::new;
    }

    @Override
    protected InferenceServiceFeatures mutateInstance(InferenceServiceFeatures instance) {
        return randomValueOtherThan(instance, InferenceServiceFeaturesTests::randomInstance);
    }

    @Override
    protected InferenceServiceFeatures mutateInstanceForVersion(InferenceServiceFeatures instance, TransportVersion version) {
        return instance;
    }

    public void testToXContent_RendersEachFeatureUnderItsName() throws IOException {
        var builder = XContentFactory.jsonBuilder();
        InferenceServiceFeatures.of(NonStreamingChatFeature.SUPPORTED_INSTANCE).toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), is(XContentHelper.stripWhitespace("""
            {
              "non_streaming_chat": {
                "supported": true
              }
            }
            """)));
    }

    public void testToXContent_EmptyFeatures() throws IOException {
        var builder = XContentFactory.jsonBuilder();
        InferenceServiceFeatures.of().toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), is("{}"));
    }

    public void testFromXContent_Empty() throws IOException {
        try (var parser = createParser(XContentType.JSON.xContent(), "{}")) {
            assertThat(InferenceServiceFeatures.fromXContent(parser).getFeatures(), is(anEmptyMap()));
        }
    }

    public void testFromXContent_ThrowsWhenTheFeatureNameIsUnknown() throws IOException {
        try (var parser = createParser(XContentType.JSON.xContent(), """
            {"not_a_real_feature": {"supported": true}}
            """)) {
            var exception = expectThrows(XContentParseException.class, () -> InferenceServiceFeatures.fromXContent(parser));
            assertThat(exception.getMessage(), containsString("[inference_features] unknown field [not_a_real_feature]"));
        }
    }

    public void testIsSupported() {
        var features = InferenceServiceFeatures.of(NonStreamingChatFeature.SUPPORTED_INSTANCE);
        assertTrue(features.isSupported(NonStreamingChatFeature.NAME));

        assertFalse(InferenceServiceFeatures.of(NonStreamingChatFeature.UNSUPPORTED_INSTANCE).isSupported(NonStreamingChatFeature.NAME));
        assertFalse(InferenceServiceFeatures.of().isSupported(NonStreamingChatFeature.NAME));
    }

    public void testGet_ReturnsNullForAFeatureThatWasNotDeclared() {
        assertNull(InferenceServiceFeatures.of().get(NonStreamingChatFeature.NAME));
        assertThat(
            InferenceServiceFeatures.of(NonStreamingChatFeature.SUPPORTED_INSTANCE).get(NonStreamingChatFeature.NAME),
            is(NonStreamingChatFeature.SUPPORTED_INSTANCE)
        );
    }

    /**
     * The point of the polymorphic map: one service can declare a single feature while another declares that feature
     * plus a type with an entirely different shape, and both round trip through the same code path.
     */
    public void testFromXContent_MixesFeatureTypesWithDifferentShapes() throws IOException {
        var json = """
            {
              "non_streaming_chat": {
                "supported": true
              },
              "test_feature": {
                "detail": "%s"
              }
            }
            """.formatted(TEST_FEATURE_DETAIL);

        var expected = InferenceServiceFeatures.of(NonStreamingChatFeature.SUPPORTED_INSTANCE, new TestFeature(TEST_FEATURE_DETAIL));

        InferenceServiceFeatures parsed;
        var parserConfig = XContentParserConfiguration.EMPTY.withRegistry(registryIncludingTestFeature());
        try (var parser = createParser(parserConfig, XContentType.JSON.xContent(), new BytesArray(json))) {
            parsed = InferenceServiceFeatures.fromXContent(parser);
        }

        assertThat(parsed, is(expected));
        assertThat(Strings.toString(parsed), is(XContentHelper.stripWhitespace(json)));
    }

    public void testWireRoundTrip_MixesFeatureTypesWithDifferentShapes() throws IOException {
        var features = InferenceServiceFeatures.of(NonStreamingChatFeature.SUPPORTED_INSTANCE, new TestFeature(TEST_FEATURE_DETAIL));

        var namedWriteables = new ArrayList<>(InferenceServiceFeatures.getNamedWriteables());
        namedWriteables.add(new NamedWriteableRegistry.Entry(InferenceFeature.class, TestFeature.NAME, TestFeature::new));

        try (var output = new BytesStreamOutput()) {
            features.writeTo(output);
            try (
                var input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), new NamedWriteableRegistry(namedWriteables))
            ) {
                assertThat(new InferenceServiceFeatures(input), is(features));
            }
        }
    }

    private static NamedXContentRegistry registryIncludingTestFeature() {
        var entries = new ArrayList<>(InferenceServiceFeatures.getNamedXContentEntries());
        entries.add(
            new NamedXContentRegistry.Entry(InferenceFeature.class, new ParseField(TestFeature.NAME), (p, c) -> TestFeature.fromXContent(p))
        );
        return new NamedXContentRegistry(entries);
    }

    /**
     * A feature that is not a {@link SupportedInferenceFeature}, to prove that a feature owns its serialized shape
     * rather than inheriting a fixed one.
     */
    private static class TestFeature implements InferenceFeature {
        private static final String NAME = "test_feature";
        private static final ParseField DETAIL_FIELD = new ParseField("detail");

        private static final ConstructingObjectParser<TestFeature, Void> PARSER = new ConstructingObjectParser<>(
            NAME,
            true,
            args -> new TestFeature((String) args[0])
        );

        static {
            PARSER.declareString(constructorArg(), DETAIL_FIELD);
        }

        static TestFeature fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        private final String detail;

        TestFeature(String detail) {
            this.detail = detail;
        }

        TestFeature(StreamInput in) throws IOException {
            this.detail = in.readString();
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(detail);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(DETAIL_FIELD.getPreferredName(), detail);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            var that = (TestFeature) o;
            return Objects.equals(detail, that.detail);
        }

        @Override
        public int hashCode() {
            return Objects.hash(detail);
        }
    }
}
