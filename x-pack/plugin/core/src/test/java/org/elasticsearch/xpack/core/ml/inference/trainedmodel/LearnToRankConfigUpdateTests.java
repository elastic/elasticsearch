/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.MlLTRNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.LearnToRankFeatureExtractorBuilder;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.QueryExtractorBuilder;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.QueryExtractorBuilderTests;
import org.elasticsearch.xpack.core.ml.utils.QueryProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfigTests.randomLearnToRankConfig;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;

public class LearnToRankConfigUpdateTests extends AbstractBWCSerializationTestCase<LearnToRankConfigUpdate> {

    public static LearnToRankConfigUpdate randomLearnToRankConfigUpdate() {
        return new LearnToRankConfigUpdate(
            randomBoolean() ? null : randomIntBetween(0, 10),
            randomBoolean()
                ? null
                : Stream.generate(
                () -> randomFrom(
                    new LearnToRankConfigTests.TestValueExtractor(randomAlphaOfLength(10)),
                    QueryExtractorBuilderTests.randomInstance()
                )
            ).limit(randomInt(5)).collect(Collectors.toList())
        );
    }

    public void testApply() throws IOException {
        LearnToRankConfig originalConfig = randomLearnToRankConfig();
        assertThat(originalConfig, equalTo(LearnToRankConfigUpdate.EMPTY_PARAMS.apply(originalConfig)));
        assertThat(
            new LearnToRankConfig.Builder(originalConfig).setNumTopFeatureImportanceValues(5).build(),
            equalTo(new LearnToRankConfigUpdate.Builder().setNumTopFeatureImportanceValues(5).build().apply(originalConfig))
        );
        assertThat(
            new LearnToRankConfig.Builder(originalConfig).setNumTopFeatureImportanceValues(1).build(),
            equalTo(new LearnToRankConfigUpdate.Builder().setNumTopFeatureImportanceValues(1).build().apply(originalConfig))
        );

        LearnToRankFeatureExtractorBuilder extractorBuilder = new LearnToRankConfigTests.TestValueExtractor("foo");
        LearnToRankFeatureExtractorBuilder extractorBuilder2 = new QueryExtractorBuilder(
            "bar",
            QueryProvider.fromParsedQuery(QueryBuilders.termQuery("foo", "bar"))
        );

        LearnToRankConfig config = new LearnToRankConfigUpdate.Builder().setNumTopFeatureImportanceValues(1)
            .setFeatureExtractorBuilders(List.of(extractorBuilder2, extractorBuilder))
            .build()
            .apply(originalConfig);
        assertThat(config.getNumTopFeatureImportanceValues(), equalTo(1));
        assertThat(extractorBuilder2, is(in(config.getFeatureExtractorBuilders())));
        assertThat(extractorBuilder, is(in(config.getFeatureExtractorBuilders())));
    }

    @Override
    protected LearnToRankConfigUpdate createTestInstance() {
        return randomLearnToRankConfigUpdate();
    }

    @Override
    protected LearnToRankConfigUpdate mutateInstance(LearnToRankConfigUpdate instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<LearnToRankConfigUpdate> instanceReader() {
        return LearnToRankConfigUpdate::new;
    }

    @Override
    protected LearnToRankConfigUpdate doParseInstance(XContentParser parser) throws IOException {
        return LearnToRankConfigUpdate.fromXContentStrict(parser);
    }

    @Override
    protected LearnToRankConfigUpdate mutateInstanceForVersion(LearnToRankConfigUpdate instance, TransportVersion version) {
        return instance;
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
                LearnToRankConfigTests.TestValueExtractor.NAME,
                LearnToRankConfigTests.TestValueExtractor::fromXContent
            )
        );
        return new NamedXContentRegistry(namedXContent);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>(new MlInferenceNamedXContentProvider().getNamedWriteables());
        namedWriteables.addAll(new MlLTRNamedXContentProvider().getNamedWriteables());
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                LearnToRankFeatureExtractorBuilder.class,
                LearnToRankConfigTests.TestValueExtractor.NAME.getPreferredName(),
                LearnToRankConfigTests.TestValueExtractor::new
            )
        );
        return new NamedWriteableRegistry(namedWriteables);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return writableRegistry();
    }
}
