/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.core.ml.utils.ToXContentParams.FOR_INTERNAL_STORAGE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TrainedModelConfigTests extends AbstractSerializingTestCase<TrainedModelConfig> {

    private boolean lenient;

    public static TrainedModelConfig.Builder createTestInstance(String modelId) {
        List<String> tags = Arrays.asList(generateRandomStringArray(randomIntBetween(0, 5), 15, false));
        return TrainedModelConfig.builder()
            .setInput(TrainedModelInputTests.createRandomInput())
            .setMetadata(randomBoolean() ? null : Collections.singletonMap(randomAlphaOfLength(10), randomAlphaOfLength(10)))
            .setCreateTime(Instant.ofEpochMilli(randomNonNegativeLong()))
            .setVersion(Version.CURRENT)
            .setModelId(modelId)
            .setCreatedBy(randomAlphaOfLength(10))
            .setDescription(randomBoolean() ? null : randomAlphaOfLength(100))
            .setEstimatedHeapMemory(randomNonNegativeLong())
            .setEstimatedOperations(randomNonNegativeLong())
            .setLicenseLevel(License.OperationMode.PLATINUM.description())
            .setTags(tags);
    }

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
        return createTestInstance(randomAlphaOfLength(10)).build();
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

    @Override
    protected ToXContent.Params getToXContentParams() {
        return lenient ? ToXContent.EMPTY_PARAMS : new ToXContent.MapParams(Collections.singletonMap(FOR_INTERNAL_STORAGE, "true"));
    }

    @Override
    protected boolean assertToXContentEquivalence() {
        return false;
    }

    public void testToXContentWithParams() throws IOException {
        TrainedModelConfig config = new TrainedModelConfig(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            Version.CURRENT,
            randomBoolean() ? null : randomAlphaOfLength(100),
            Instant.ofEpochMilli(randomNonNegativeLong()),
            TrainedModelDefinitionTests.createRandomBuilder(randomAlphaOfLength(10)).build(),
            Collections.emptyList(),
            randomBoolean() ? null : Collections.singletonMap(randomAlphaOfLength(10), randomAlphaOfLength(10)),
            TrainedModelInputTests.createRandomInput(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            "platinum");

        BytesReference reference = XContentHelper.toXContent(config, XContentType.JSON, ToXContent.EMPTY_PARAMS, false);
        assertThat(reference.utf8ToString(), containsString("definition"));

        reference = XContentHelper.toXContent(config,
            XContentType.JSON,
            new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true")),
            false);
        assertThat(reference.utf8ToString(), not(containsString("definition")));
    }

    public void testValidateWithNullDefinition() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> TrainedModelConfig.builder().validate());
        assertThat(ex.getMessage(), equalTo("[definition] must not be null."));
    }

    public void testValidateWithInvalidID() {
        String modelId = "InvalidID-";
        ElasticsearchException ex = expectThrows(ElasticsearchException.class,
            () -> TrainedModelConfig.builder()
                .setDefinition(TrainedModelDefinitionTests.createRandomBuilder(modelId))
                .setModelId(modelId).validate());
        assertThat(ex.getMessage(), equalTo(Messages.getMessage(Messages.INVALID_ID, "model_id", modelId)));
    }

    public void testValidateWithLongID() {
        String modelId = IntStream.range(0, 100).mapToObj(x -> "a").collect(Collectors.joining());
        ElasticsearchException ex = expectThrows(ElasticsearchException.class,
            () -> TrainedModelConfig.builder()
                .setDefinition(TrainedModelDefinitionTests.createRandomBuilder(modelId))
                .setModelId(modelId).validate());
        assertThat(ex.getMessage(), equalTo(Messages.getMessage(Messages.ID_TOO_LONG, "model_id", modelId, MlStrings.ID_LENGTH_LIMIT)));
    }

    public void testValidateWithIllegallyUserProvidedFields() {
        String modelId = "simplemodel";
        ElasticsearchException ex = expectThrows(ElasticsearchException.class,
            () -> TrainedModelConfig.builder()
                .setDefinition(TrainedModelDefinitionTests.createRandomBuilder(modelId))
                .setCreateTime(Instant.now())
                .setModelId(modelId).validate());
        assertThat(ex.getMessage(), equalTo("illegal to set [create_time] at inference model creation"));

        ex = expectThrows(ElasticsearchException.class,
            () -> TrainedModelConfig.builder()
                .setDefinition(TrainedModelDefinitionTests.createRandomBuilder(modelId))
                .setVersion(Version.CURRENT)
                .setModelId(modelId).validate());
        assertThat(ex.getMessage(), equalTo("illegal to set [version] at inference model creation"));

        ex = expectThrows(ElasticsearchException.class,
            () -> TrainedModelConfig.builder()
                .setDefinition(TrainedModelDefinitionTests.createRandomBuilder(modelId))
                .setCreatedBy("ml_user")
                .setModelId(modelId).validate());
        assertThat(ex.getMessage(), equalTo("illegal to set [created_by] at inference model creation"));
    }

    public void testIsAvailableWithLicense() {
        TrainedModelConfig.Builder builder = createTestInstance(randomAlphaOfLength(10));

        XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.isActive()).thenReturn(false);
        when(licenseState.getOperationMode()).thenReturn(License.OperationMode.BASIC);

        assertFalse(builder.setLicenseLevel(License.OperationMode.PLATINUM.description()).build().isAvailableWithLicense(licenseState));
        assertTrue(builder.setLicenseLevel(License.OperationMode.BASIC.description()).build().isAvailableWithLicense(licenseState));


        when(licenseState.isActive()).thenReturn(true);
        when(licenseState.getOperationMode()).thenReturn(License.OperationMode.PLATINUM);
        assertTrue(builder.setLicenseLevel(License.OperationMode.PLATINUM.description()).build().isAvailableWithLicense(licenseState));
        assertTrue(builder.setLicenseLevel(License.OperationMode.BASIC.description()).build().isAvailableWithLicense(licenseState));
        assertTrue(builder.setLicenseLevel(License.OperationMode.GOLD.description()).build().isAvailableWithLicense(licenseState));

        when(licenseState.isActive()).thenReturn(false);
        assertFalse(builder.setLicenseLevel(License.OperationMode.PLATINUM.description()).build().isAvailableWithLicense(licenseState));
        assertTrue(builder.setLicenseLevel(License.OperationMode.BASIC.description()).build().isAvailableWithLicense(licenseState));
        assertFalse(builder.setLicenseLevel(License.OperationMode.GOLD.description()).build().isAvailableWithLicense(licenseState));

        when(licenseState.isActive()).thenReturn(true);
        when(licenseState.getOperationMode()).thenReturn(License.OperationMode.GOLD);
        assertFalse(builder.setLicenseLevel(License.OperationMode.PLATINUM.description()).build().isAvailableWithLicense(licenseState));
        assertTrue(builder.setLicenseLevel(License.OperationMode.BASIC.description()).build().isAvailableWithLicense(licenseState));
        assertTrue(builder.setLicenseLevel(License.OperationMode.GOLD.description()).build().isAvailableWithLicense(licenseState));

        when(licenseState.isActive()).thenReturn(false);
        assertFalse(builder.setLicenseLevel(License.OperationMode.PLATINUM.description()).build().isAvailableWithLicense(licenseState));
        assertTrue(builder.setLicenseLevel(License.OperationMode.BASIC.description()).build().isAvailableWithLicense(licenseState));
        assertFalse(builder.setLicenseLevel(License.OperationMode.GOLD.description()).build().isAvailableWithLicense(licenseState));
    }

}
