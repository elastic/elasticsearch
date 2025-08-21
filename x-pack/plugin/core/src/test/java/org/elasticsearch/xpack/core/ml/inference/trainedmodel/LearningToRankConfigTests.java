/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.license.License;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.InferenceConfigItemTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.LearningToRankFeatureExtractorBuilder;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.QueryExtractorBuilderTests;
import org.elasticsearch.xpack.core.ml.ltr.MlLTRNamedXContentProvider;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.hamcrest.Matchers.is;

public class LearningToRankConfigTests extends InferenceConfigItemTestCase<LearningToRankConfig> {
    private boolean lenient;

    public static LearningToRankConfig randomLearningToRankConfig() {
        return new LearningToRankConfig(
            randomBoolean() ? null : randomIntBetween(0, 10),
            randomBoolean()
                ? null
                : Stream.generate(QueryExtractorBuilderTests::randomInstance).limit(randomInt(5)).collect(Collectors.toList()),
            randomBoolean() ? null : randomMap(0, 10, () -> Tuple.tuple(randomIdentifier(), randomIdentifier()))
        );
    }

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected LearningToRankConfig createTestInstance() {
        return randomLearningToRankConfig();
    }

    @Override
    protected LearningToRankConfig mutateInstance(LearningToRankConfig instance) {
        int i = randomInt(2);

        LearningToRankConfig.Builder builder = LearningToRankConfig.builder(instance);

        switch (i) {
            case 0 -> {
                builder.setNumTopFeatureImportanceValues(
                    randomValueOtherThan(
                        instance.getNumTopFeatureImportanceValues(),
                        () -> randomBoolean() && instance.getNumTopFeatureImportanceValues() != 0 ? null : randomIntBetween(0, 10)
                    )
                );
            }
            case 1 -> {
                builder.setLearningToRankFeatureExtractorBuilders(
                    randomValueOtherThan(
                        instance.getFeatureExtractorBuilders(),
                        () -> randomBoolean() || instance.getFeatureExtractorBuilders().isEmpty()
                            ? Stream.generate(QueryExtractorBuilderTests::randomInstance)
                                .limit(randomIntBetween(1, 5))
                                .collect(Collectors.toList())
                            : null
                    )
                );
            }
            case 2 -> {
                builder.setParamsDefaults(
                    randomValueOtherThan(
                        instance.getParamsDefaults(),
                        () -> randomBoolean() || instance.getParamsDefaults().isEmpty()
                            ? randomMap(1, 10, () -> Tuple.tuple(randomIdentifier(), randomIdentifier()))
                            : null
                    )
                );
            }
            default -> throw new AssertionError("Unexpected random test case");
        }

        return builder.build();
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.isEmpty() == false;
    }

    @Override
    protected Writeable.Reader<LearningToRankConfig> instanceReader() {
        return LearningToRankConfig::new;
    }

    @Override
    protected LearningToRankConfig doParseInstance(XContentParser parser) throws IOException {
        return lenient ? LearningToRankConfig.fromXContentLenient(parser) : LearningToRankConfig.fromXContentStrict(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected LearningToRankConfig mutateInstanceForVersion(LearningToRankConfig instance, TransportVersion version) {
        return instance;
    }

    public void testDuplicateFeatureNames() {
        List<LearningToRankFeatureExtractorBuilder> featureExtractorBuilderList = List.of(
            new TestValueExtractor("foo"),
            new TestValueExtractor("foo")
        );

        LearningToRankConfig.Builder builder = LearningToRankConfig.builder(randomLearningToRankConfig())
            .setLearningToRankFeatureExtractorBuilders(featureExtractorBuilderList);

        expectThrows(IllegalArgumentException.class, () -> builder.build());
    }

    public void testLicenseSupport_ForPutAction_RequiresEnterprise() {
        var config = randomLearningToRankConfig();
        assertThat(config.getMinLicenseSupportedForAction(RestRequest.Method.PUT), is(License.OperationMode.ENTERPRISE));
    }

    public void testLicenseSupport_ForGetAction_RequiresPlatinum() {
        var config = randomLearningToRankConfig();
        assertThat(config.getMinLicenseSupportedForAction(RestRequest.Method.GET), is(License.OperationMode.PLATINUM));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new MlLTRNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents());
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                LearningToRankFeatureExtractorBuilder.class,
                TestValueExtractor.NAME,
                TestValueExtractor::fromXContent
            )
        );
        return new NamedXContentRegistry(namedXContent);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>(new MlInferenceNamedXContentProvider().getNamedWriteables());
        namedWriteables.addAll(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables());
        namedWriteables.addAll(new MlLTRNamedXContentProvider().getNamedWriteables());
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                LearningToRankFeatureExtractorBuilder.class,
                TestValueExtractor.NAME.getPreferredName(),
                TestValueExtractor::new
            )
        );
        return new NamedWriteableRegistry(namedWriteables);
    }

    private static class TestValueExtractor implements LearningToRankFeatureExtractorBuilder {
        public static final ParseField NAME = new ParseField("test");
        private final String featureName;

        private static final ConstructingObjectParser<TestValueExtractor, Void> PARSER = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            a -> new TestValueExtractor((String) a[0])
        );
        private static final ConstructingObjectParser<TestValueExtractor, Void> LENIENT_PARSER = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            true,
            a -> new TestValueExtractor((String) a[0])
        );
        static {
            PARSER.declareString(constructorArg(), FEATURE_NAME);
            LENIENT_PARSER.declareString(constructorArg(), FEATURE_NAME);
        }

        public static TestValueExtractor fromXContent(XContentParser parser, Object context) {
            boolean lenient = Boolean.TRUE.equals(context);
            return lenient ? LENIENT_PARSER.apply(parser, null) : PARSER.apply(parser, null);
        }

        TestValueExtractor(StreamInput in) throws IOException {
            this.featureName = in.readString();
        }

        TestValueExtractor(String featureName) {
            this.featureName = featureName;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(FEATURE_NAME.getPreferredName(), featureName);
            builder.endObject();
            return builder;
        }

        @Override
        public String getWriteableName() {
            return NAME.getPreferredName();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(featureName);
        }

        @Override
        public String featureName() {
            return featureName;
        }

        @Override
        public void validate() throws Exception {}

        @Override
        public String getName() {
            return NAME.getPreferredName();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestValueExtractor that = (TestValueExtractor) o;
            return Objects.equals(featureName, that.featureName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(featureName);
        }

        @Override
        public TestValueExtractor rewrite(QueryRewriteContext ctx) throws IOException {
            return this;
        }
    }
}
