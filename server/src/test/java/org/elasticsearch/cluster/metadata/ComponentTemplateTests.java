/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfigurationTests;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.SimpleDiffableSerializationTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ComponentTemplateTests extends SimpleDiffableSerializationTestCase<ComponentTemplate> {
    @Override
    protected ComponentTemplate makeTestChanges(ComponentTemplate testInstance) {
        return mutateInstance(testInstance);
    }

    @Override
    protected Writeable.Reader<Diff<ComponentTemplate>> diffReader() {
        return ComponentTemplate::readComponentTemplateDiffFrom;
    }

    @Override
    protected ComponentTemplate doParseInstance(XContentParser parser) throws IOException {
        return ComponentTemplate.parse(parser);
    }

    @Override
    protected Writeable.Reader<ComponentTemplate> instanceReader() {
        return ComponentTemplate::new;
    }

    @Override
    protected ComponentTemplate createTestInstance() {
        return randomInstance(true);
    }

    // In many cases the index template is used with indices adding lifecycle would render it invalid that's why we
    // do not always want to randomly add a lifecycle.
    public static ComponentTemplate randomInstance() {
        return randomInstance(false);
    }

    public static ComponentTemplate randomInstance(boolean lifecycleAllowed) {
        Settings settings = null;
        CompressedXContent mappings = null;
        Map<String, AliasMetadata> aliases = null;
        DataStreamLifecycle lifecycle = null;
        if (randomBoolean()) {
            settings = randomSettings();
        }
        if (randomBoolean()) {
            mappings = randomMappings();
        }
        if (randomBoolean()) {
            aliases = randomAliases();
        }
        if (randomBoolean() && lifecycleAllowed) {
            lifecycle = randomLifecycle();
        }
        Template template = new Template(settings, mappings, aliases, lifecycle);

        Map<String, Object> meta = null;
        if (randomBoolean()) {
            meta = randomMeta();
        }
        return new ComponentTemplate(template, randomBoolean() ? null : randomNonNegativeLong(), meta);
    }

    public static Map<String, AliasMetadata> randomAliases() {
        String aliasName = randomAlphaOfLength(5);
        AliasMetadata aliasMeta = AliasMetadata.builder(aliasName)
            .filter("{\"term\":{\"year\":" + randomIntBetween(1, 3000) + "}}")
            .routing(randomBoolean() ? null : randomAlphaOfLength(3))
            .isHidden(randomBoolean() ? null : randomBoolean())
            .writeIndex(randomBoolean() ? null : randomBoolean())
            .build();
        return Collections.singletonMap(aliasName, aliasMeta);
    }

    private static CompressedXContent randomMappings() {
        try {
            return new CompressedXContent("{\"properties\":{\"" + randomAlphaOfLength(5) + "\":{\"type\":\"keyword\"}}}");
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

    private static DataStreamLifecycle randomLifecycle() {
        return rarely() ? Template.NO_LIFECYCLE : DataStreamLifecycleTests.randomLifecycle();
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

    @Override
    protected ComponentTemplate mutateInstance(ComponentTemplate orig) {
        return mutateTemplate(orig);
    }

    public static ComponentTemplate mutateTemplate(ComponentTemplate orig) {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> {
                Template ot = orig.template();
                yield switch (randomIntBetween(0, 3)) {
                    case 0 -> new ComponentTemplate(
                        new Template(
                            randomValueOtherThan(ot.settings(), ComponentTemplateTests::randomSettings),
                            ot.mappings(),
                            ot.aliases(),
                            ot.lifecycle()
                        ),
                        orig.version(),
                        orig.metadata()
                    );
                    case 1 -> new ComponentTemplate(
                        new Template(
                            ot.settings(),
                            randomValueOtherThan(ot.mappings(), ComponentTemplateTests::randomMappings),
                            ot.aliases(),
                            ot.lifecycle()
                        ),
                        orig.version(),
                        orig.metadata()
                    );
                    case 2 -> new ComponentTemplate(
                        new Template(
                            ot.settings(),
                            ot.mappings(),
                            randomValueOtherThan(ot.aliases(), ComponentTemplateTests::randomAliases),
                            ot.lifecycle()
                        ),
                        orig.version(),
                        orig.metadata()
                    );
                    case 3 -> new ComponentTemplate(
                        new Template(
                            ot.settings(),
                            ot.mappings(),
                            ot.aliases(),
                            randomValueOtherThan(ot.lifecycle(), ComponentTemplateTests::randomLifecycle)
                        ),
                        orig.version(),
                        orig.metadata()
                    );
                    default -> throw new IllegalStateException("illegal randomization branch");
                };
            }
            case 1 -> new ComponentTemplate(
                orig.template(),
                randomValueOtherThan(orig.version(), ESTestCase::randomNonNegativeLong),
                orig.metadata()
            );
            case 2 -> new ComponentTemplate(
                orig.template(),
                orig.version(),
                randomValueOtherThan(orig.metadata(), ComponentTemplateTests::randomMeta)
            );
            default -> throw new IllegalStateException("illegal randomization branch");
        };
    }

    public void testMappingsEquals() throws IOException {
        {
            CompressedXContent mappings = randomMappings();
            assertThat(Template.mappingsEquals(mappings, mappings), equalTo(true));
        }

        {
            assertThat(Template.mappingsEquals(null, null), equalTo(true));
        }

        {
            CompressedXContent mappings = randomMappings();
            assertThat(Template.mappingsEquals(mappings, null), equalTo(false));
            assertThat(Template.mappingsEquals(null, mappings), equalTo(false));
        }

        {
            String randomString = randomAlphaOfLength(10);
            CompressedXContent m1 = new CompressedXContent(Strings.format("""
                {"properties":{"%s":{"type":"keyword"}}}
                """, randomString));
            CompressedXContent m2 = new CompressedXContent(Strings.format("""
                {"properties":{"%s":{"type":"keyword"}}}
                """, randomString));
            assertThat(Template.mappingsEquals(m1, m2), equalTo(true));
        }

        {
            CompressedXContent m1 = randomMappings();
            CompressedXContent m2 = new CompressedXContent(Strings.format("""
                {"properties":{"%s":{"type":"keyword"}}}
                """, randomAlphaOfLength(10)));
            assertThat(Template.mappingsEquals(m1, m2), equalTo(false));
        }

        {
            Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(Strings.format("""
                {"%s":{"properties":{"%s":{"type":"keyword"}}}}
                """, MapperService.SINGLE_MAPPING_NAME, randomAlphaOfLength(10))), true, XContentType.JSON).v2();
            Map<String, Object> reduceMap = Template.reduceMapping(map);
            CompressedXContent m1 = new CompressedXContent(Strings.toString(XContentFactory.jsonBuilder().map(map)));
            CompressedXContent m2 = new CompressedXContent(Strings.toString(XContentFactory.jsonBuilder().map(reduceMap)));
            assertThat(Template.mappingsEquals(m1, m2), equalTo(true));
        }
    }

    public void testXContentSerializationWithRollover() throws IOException {
        Settings settings = null;
        CompressedXContent mappings = null;
        Map<String, AliasMetadata> aliases = null;
        if (randomBoolean()) {
            settings = randomSettings();
        }
        if (randomBoolean()) {
            mappings = randomMappings();
        }
        if (randomBoolean()) {
            aliases = randomAliases();
        }
        DataStreamLifecycle lifecycle = new DataStreamLifecycle(randomMillisUpToYear9999());
        ComponentTemplate template = new ComponentTemplate(
            new Template(settings, mappings, aliases, lifecycle),
            randomNonNegativeLong(),
            null
        );

        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.humanReadable(true);
            RolloverConfiguration rolloverConfiguration = RolloverConfigurationTests.randomRolloverConditions();
            template.toXContent(builder, ToXContent.EMPTY_PARAMS, rolloverConfiguration);
            String serialized = Strings.toString(builder);
            assertThat(serialized, containsString("rollover"));
            for (String label : rolloverConfiguration.resolveRolloverConditions(lifecycle.getEffectiveDataRetention())
                .getConditions()
                .keySet()) {
                assertThat(serialized, containsString(label));
            }
        }
    }
}
