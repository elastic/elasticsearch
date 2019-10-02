/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeTests;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;


public class TrainedModelConfigTests extends AbstractSerializingTestCase<TrainedModelConfig> {

    private boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected TrainedModelConfig doParseInstance(XContentParser parser) throws IOException {
        return TrainedModelConfig.fromXContent(parser, lenient).build();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> !field.isEmpty();
    }

    @Override
    protected TrainedModelConfig createTestInstance() {
        return new TrainedModelConfig(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            Version.CURRENT,
            randomBoolean() ? null : randomAlphaOfLength(100),
            Instant.ofEpochMilli(randomNonNegativeLong()),
            randomBoolean() ? null : randomNonNegativeLong(),
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomFrom(TreeTests.createRandom()),
            randomBoolean() ? null : Collections.singletonMap(randomAlphaOfLength(10), randomAlphaOfLength(10)));
    }

    @Override
    protected Writeable.Reader<TrainedModelConfig> instanceReader() {
        return TrainedModelConfig::new;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    public void testValidateWithNullDefinition() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> TrainedModelConfig.builder().validate());
        assertThat(ex.getMessage(), equalTo("[definition] must not be null."));
    }

    public void testValidateWithInvalidID() {
        String modelId = "InvalidID-";
        ElasticsearchException ex = expectThrows(ElasticsearchException.class,
            () -> TrainedModelConfig.builder().setDefinition(randomFrom(TreeTests.createRandom())).setModelId(modelId).validate());
        assertThat(ex.getMessage(), equalTo(Messages.getMessage(Messages.INVALID_ID, "model_id", modelId)));
    }

    public void testValidateWithLongID() {
        String modelId = IntStream.range(0, 100).mapToObj(x -> "a").collect(Collectors.joining());
        ElasticsearchException ex = expectThrows(ElasticsearchException.class,
            () -> TrainedModelConfig.builder().setDefinition(randomFrom(TreeTests.createRandom())).setModelId(modelId).validate());
        assertThat(ex.getMessage(), equalTo(Messages.getMessage(Messages.ID_TOO_LONG, "model_id", modelId, MlStrings.ID_LENGTH_LIMIT)));
    }

    public void testValidateWithIllegallyUserProvidedFields() {
        String modelId = "simplemodel";
        ElasticsearchException ex = expectThrows(ElasticsearchException.class,
            () -> TrainedModelConfig.builder()
                .setDefinition(randomFrom(TreeTests.createRandom()))
                .setCreatedTime(Instant.now())
                .setModelId(modelId).validate());
        assertThat(ex.getMessage(), equalTo("illegal to set [created_time] at inference model creation"));

        ex = expectThrows(ElasticsearchException.class,
            () -> TrainedModelConfig.builder()
                .setDefinition(randomFrom(TreeTests.createRandom()))
                .setVersion(Version.CURRENT)
                .setModelId(modelId).validate());
        assertThat(ex.getMessage(), equalTo("illegal to set [version] at inference model creation"));

        ex = expectThrows(ElasticsearchException.class,
            () -> TrainedModelConfig.builder()
                .setDefinition(randomFrom(TreeTests.createRandom()))
                .setCreatedBy("ml_user")
                .setModelId(modelId).validate());
        assertThat(ex.getMessage(), equalTo("illegal to set [created_by] at inference model creation"));
    }
}
