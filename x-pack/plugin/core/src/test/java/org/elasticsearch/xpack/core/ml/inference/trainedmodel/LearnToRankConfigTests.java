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
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.InferenceConfigItemTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.MlLTRNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.LearnToRankFeatureExtractorBuilder;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.QueryExtractorBuilderTests;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class LearnToRankConfigTests extends InferenceConfigItemTestCase<LearnToRankConfig> {
    private boolean lenient;

    public static LearnToRankConfig randomLearnToRankConfig() {
        return new LearnToRankConfig(
            randomBoolean() ? null : randomIntBetween(0, 10),
            randomBoolean()
                ? null
                : Stream.generate(
                    () -> randomFrom(
                        new TestValueExtractor(randomAlphaOfLength(10)),
                        QueryExtractorBuilderTests.randomInstance()
                    )
            ).limit(randomInt(5)).collect(Collectors.toList())
        );
    }

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected LearnToRankConfig createTestInstance() {
        return randomLearnToRankConfig();
    }

    @Override
    protected LearnToRankConfig mutateInstance(LearnToRankConfig instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.isEmpty() == false;
    }

    @Override
    protected Writeable.Reader<LearnToRankConfig> instanceReader() {
        return LearnToRankConfig::new;
    }

    @Override
    protected LearnToRankConfig doParseInstance(XContentParser parser) throws IOException {
        return lenient ? LearnToRankConfig.fromXContentLenient(parser) : LearnToRankConfig.fromXContentStrict(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected LearnToRankConfig mutateInstanceForVersion(LearnToRankConfig instance, TransportVersion version) {
        return instance;
    }

    public void testDuplicateFeatureNames() {
        List<LearnToRankFeatureExtractorBuilder> featureExtractorBuilderList = List.of(
            new TestValueExtractor("foo"),
            new TestValueExtractor("foo")
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new LearnToRankConfig(randomBoolean() ? null : randomIntBetween(0, 10), featureExtractorBuilderList)
        );
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new MlLTRNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                LearnToRankFeatureExtractorBuilder.class,
                TestValueExtractor.NAME,
                TestValueExtractor::fromXContent
            )
        );
        return new NamedXContentRegistry(namedXContent);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>(new MlInferenceNamedXContentProvider().getNamedWriteables());
        namedWriteables.addAll(new MlLTRNamedXContentProvider().getNamedWriteables());
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                LearnToRankFeatureExtractorBuilder.class,
                TestValueExtractor.NAME.getPreferredName(),
                TestValueExtractor::new
            )
        );
        return new NamedWriteableRegistry(namedWriteables);
    }

    static class TestValueExtractor implements LearnToRankFeatureExtractorBuilder {
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
    }
}
