/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfigurationTests;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.SimpleDiffableSerializationTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.DataStream.TIMESTAMP_FIELD_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class ComposableIndexTemplateTests extends SimpleDiffableSerializationTestCase<ComposableIndexTemplate> {
    @Override
    protected ComposableIndexTemplate makeTestChanges(ComposableIndexTemplate testInstance) {
        return mutateInstance(testInstance);
    }

    @Override
    protected Writeable.Reader<Diff<ComposableIndexTemplate>> diffReader() {
        return ComposableIndexTemplate::readITV2DiffFrom;
    }

    @Override
    protected ComposableIndexTemplate doParseInstance(XContentParser parser) throws IOException {
        return ComposableIndexTemplate.parse(parser);
    }

    @Override
    protected Writeable.Reader<ComposableIndexTemplate> instanceReader() {
        return ComposableIndexTemplate::new;
    }

    @Override
    protected ComposableIndexTemplate createTestInstance() {
        return randomInstance();
    }

    public static ComposableIndexTemplate randomInstance() {
        Template template = null;
        ComposableIndexTemplate.DataStreamTemplate dataStreamTemplate = randomDataStreamTemplate();
        Template.Builder builder = Template.builder();
        if (dataStreamTemplate != null || randomBoolean()) {
            if (randomBoolean()) {
                builder.settings(randomSettings());
            }
            if (dataStreamTemplate != null || randomBoolean()) {
                builder.mappings(randomMappings(dataStreamTemplate));
            }
            if (dataStreamTemplate == null && randomBoolean()) {
                builder.aliases(randomAliases());
            }
            if (dataStreamTemplate != null && randomBoolean()) {
                builder.lifecycle(DataStreamLifecycleTemplateTests.randomDataLifecycleTemplate());
            }
            template = builder.build();
        }

        Map<String, Object> meta = null;
        if (randomBoolean()) {
            meta = randomMeta();
        }

        List<String> indexPatterns = randomList(1, 4, () -> randomAlphaOfLength(4));
        List<String> ignoreMissingComponentTemplates = randomList(0, 4, () -> randomAlphaOfLength(4));
        return ComposableIndexTemplate.builder()
            .indexPatterns(indexPatterns)
            .template(template)
            .componentTemplates(randomBoolean() ? null : randomList(0, 10, () -> randomAlphaOfLength(5)))
            .priority(randomBoolean() ? null : randomNonNegativeLong())
            .version(randomBoolean() ? null : randomNonNegativeLong())
            .metadata(meta)
            .dataStreamTemplate(dataStreamTemplate)
            .allowAutoCreate(randomOptionalBoolean())
            .ignoreMissingComponentTemplates(ignoreMissingComponentTemplates)
            .deprecated(randomOptionalBoolean())
            .build();
    }

    private static Map<String, AliasMetadata> randomAliases() {
        String aliasName = randomAlphaOfLength(5);
        AliasMetadata aliasMeta = AliasMetadata.builder(aliasName)
            .filter("{\"term\":{\"year\":" + randomIntBetween(1, 3000) + "}}")
            .routing(randomBoolean() ? null : randomAlphaOfLength(3))
            .isHidden(randomBoolean() ? null : randomBoolean())
            .writeIndex(randomBoolean() ? null : randomBoolean())
            .build();
        return Collections.singletonMap(aliasName, aliasMeta);
    }

    private static CompressedXContent randomMappings(ComposableIndexTemplate.DataStreamTemplate dataStreamTemplate) {
        try {
            if (dataStreamTemplate != null) {
                return new CompressedXContent("{\"properties\":{\"" + TIMESTAMP_FIELD_NAME + "\":{\"type\":\"date\"}}}");
            } else {
                return new CompressedXContent("{\"properties\":{\"" + randomAlphaOfLength(5) + "\":{\"type\":\"keyword\"}}}");
            }
        } catch (IOException e) {
            fail("got an IO exception creating fake mappings: " + e);
            return null;
        }
    }

    private static Settings randomSettings() {
        return indexSettings(randomIntBetween(1, 10), randomIntBetween(0, 5)).put(IndexMetadata.SETTING_BLOCKS_READ, randomBoolean())
            .put(IndexMetadata.SETTING_BLOCKS_WRITE, randomBoolean())
            .put(IndexMetadata.SETTING_BLOCKS_WRITE, randomBoolean())
            .put(IndexMetadata.SETTING_PRIORITY, randomIntBetween(0, 100000))
            .build();
    }

    private static Map<String, Object> randomMeta() {
        if (randomBoolean()) {
            return Collections.singletonMap(randomAlphaOfLength(4), randomAlphaOfLength(4));
        } else {
            return Collections.singletonMap(
                randomAlphaOfLength(5),
                Collections.singletonMap(randomAlphaOfLength(4), randomAlphaOfLength(4))
            );
        }
    }

    private static ComposableIndexTemplate.DataStreamTemplate randomDataStreamTemplate() {
        if (randomBoolean()) {
            return null;
        } else {
            return DataStreamTemplateTests.randomInstance();
        }
    }

    @Override
    protected ComposableIndexTemplate mutateInstance(ComposableIndexTemplate orig) {
        return mutateTemplate(orig);
    }

    public static ComposableIndexTemplate mutateTemplate(ComposableIndexTemplate orig) {
        switch (randomIntBetween(0, 8)) {
            case 0:
                List<String> newIndexPatterns = randomValueOtherThan(
                    orig.indexPatterns(),
                    () -> randomList(1, 4, () -> randomAlphaOfLength(4))
                );
                return orig.toBuilder().indexPatterns(newIndexPatterns).build();
            case 1:
                return orig.toBuilder()
                    .template(
                        randomValueOtherThan(
                            orig.template(),
                            () -> Template.builder()
                                .settings(randomSettings())
                                .mappings(randomMappings(orig.getDataStreamTemplate()))
                                .aliases(randomAliases())
                                .lifecycle(
                                    orig.getDataStreamTemplate() == null
                                        ? null
                                        : DataStreamLifecycleTemplateTests.randomDataLifecycleTemplate()
                                )
                                .build()
                        )
                    )
                    .build();
            case 2:
                List<String> newComposedOf = randomValueOtherThan(orig.composedOf(), () -> randomList(0, 10, () -> randomAlphaOfLength(5)));
                return orig.toBuilder().componentTemplates(newComposedOf).build();
            case 3:
                return orig.toBuilder().priority(randomValueOtherThan(orig.priority(), ESTestCase::randomNonNegativeLong)).build();
            case 4:
                return orig.toBuilder().version(randomValueOtherThan(orig.version(), ESTestCase::randomNonNegativeLong)).build();
            case 5:
                return orig.toBuilder().metadata(randomValueOtherThan(orig.metadata(), ComposableIndexTemplateTests::randomMeta)).build();
            case 6:
                return orig.toBuilder()
                    .dataStreamTemplate(
                        randomValueOtherThan(orig.getDataStreamTemplate(), ComposableIndexTemplateTests::randomDataStreamTemplate)
                    )
                    .build();
            case 7:
                List<String> ignoreMissingComponentTemplates = randomValueOtherThan(
                    orig.getIgnoreMissingComponentTemplates(),
                    () -> randomList(1, 4, () -> randomAlphaOfLength(4))
                );
                return orig.toBuilder().ignoreMissingComponentTemplates(ignoreMissingComponentTemplates).build();
            case 8:
                return orig.toBuilder().deprecated(orig.isDeprecated() ? randomFrom(false, null) : true).build();
            default:
                throw new IllegalStateException("illegal randomization branch");
        }
    }

    public void testComponentTemplatesEquals() {
        assertThat(ComposableIndexTemplate.componentTemplatesEquals(null, null), equalTo(true));
        assertThat(ComposableIndexTemplate.componentTemplatesEquals(null, List.of()), equalTo(true));
        assertThat(ComposableIndexTemplate.componentTemplatesEquals(List.of(), null), equalTo(true));
        assertThat(ComposableIndexTemplate.componentTemplatesEquals(List.of(), List.of()), equalTo(true));
        assertThat(ComposableIndexTemplate.componentTemplatesEquals(List.of(randomAlphaOfLength(5)), List.of()), equalTo(false));
        assertThat(ComposableIndexTemplate.componentTemplatesEquals(List.of(), List.of(randomAlphaOfLength(5))), equalTo(false));
    }

    public void testXContentSerializationWithRolloverAndEffectiveRetention() throws IOException {
        Settings settings = null;
        CompressedXContent mappings = null;
        Map<String, AliasMetadata> aliases = null;
        DataStreamOptions.Template dataStreamOptions = null;
        ComposableIndexTemplate.DataStreamTemplate dataStreamTemplate = randomDataStreamTemplate();
        if (randomBoolean()) {
            settings = randomSettings();
        }
        if (randomBoolean()) {
            mappings = randomMappings(dataStreamTemplate);
        }
        if (randomBoolean()) {
            aliases = randomAliases();
        }
        if (randomBoolean()) {
            // Do not set random lifecycle to avoid having data_retention and effective_retention in the response.
            dataStreamOptions = new DataStreamOptions.Template(DataStreamFailureStore.builder().enabled(randomBoolean()).buildTemplate());
        }
        // We use the empty lifecycle so the global retention can be in effect
        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.Template.DATA_DEFAULT;
        Template template = new Template(settings, mappings, aliases, lifecycle, dataStreamOptions);
        ComposableIndexTemplate.builder()
            .indexPatterns(List.of(randomAlphaOfLength(4)))
            .template(template)
            .componentTemplates(List.of())
            .priority(randomNonNegativeLong())
            .version(randomNonNegativeLong())
            .dataStreamTemplate(dataStreamTemplate)
            .build();

        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.humanReadable(true);
            RolloverConfiguration rolloverConfiguration = RolloverConfigurationTests.randomRolloverConditions();
            DataStreamGlobalRetention globalRetention = DataStreamGlobalRetentionTests.randomGlobalRetention();
            ToXContent.Params withEffectiveRetention = new ToXContent.MapParams(DataStreamLifecycle.INCLUDE_EFFECTIVE_RETENTION_PARAMS);
            template.toXContent(builder, withEffectiveRetention, rolloverConfiguration);
            String serialized = Strings.toString(builder);
            assertThat(serialized, containsString("rollover"));
            for (String label : rolloverConfiguration.resolveRolloverConditions(
                lifecycle.toDataStreamLifecycle().getEffectiveDataRetention(globalRetention, randomBoolean())
            ).getConditions().keySet()) {
                assertThat(serialized, containsString(label));
            }
            /*
             * A template does not have a global retention and the lifecycle has no retention, so there will be no data_retention or
             * effective_retention.
             */
            assertThat(serialized, not(containsString("data_retention")));
            assertThat(serialized, not(containsString("effective_retention")));
        }
    }

    public void testBuilderRoundtrip() {
        ComposableIndexTemplate template = randomInstance();
        assertEquals(template, template.toBuilder().build());

        if (template.template() != null) {
            assertEquals(template.template(), Template.builder(template.template()).build());
        }
    }
}
