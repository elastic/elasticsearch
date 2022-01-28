/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

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
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ComponentTemplateTests extends SimpleDiffableSerializationTestCase<ComponentTemplate> {
    @Override
    protected ComponentTemplate makeTestChanges(ComponentTemplate testInstance) {
        try {
            return mutateInstance(testInstance);
        } catch (IOException e) {
            logger.error(e);
            fail("mutating should not throw an exception, but got: " + e);
            return null;
        }
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
        return randomInstance();
    }

    public static ComponentTemplate randomInstance() {
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
        Template template = new Template(settings, mappings, aliases);

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
        return Settings.builder()
            .put(IndexMetadata.SETTING_BLOCKS_READ, randomBoolean())
            .put(IndexMetadata.SETTING_BLOCKS_WRITE, randomBoolean())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 10))
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 5))
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

    @Override
    protected ComponentTemplate mutateInstance(ComponentTemplate orig) throws IOException {
        return mutateTemplate(orig);
    }

    public static ComponentTemplate mutateTemplate(ComponentTemplate orig) {
        switch (randomIntBetween(0, 2)) {
            case 0:
                switch (randomIntBetween(0, 2)) {
                    case 0 -> {
                        Template ot = orig.template();
                        return new ComponentTemplate(
                            new Template(
                                randomValueOtherThan(ot.settings(), ComponentTemplateTests::randomSettings),
                                ot.mappings(),
                                ot.aliases()
                            ),
                            orig.version(),
                            orig.metadata()
                        );
                    }
                    case 1 -> {
                        Template ot2 = orig.template();
                        return new ComponentTemplate(
                            new Template(
                                ot2.settings(),
                                randomValueOtherThan(ot2.mappings(), ComponentTemplateTests::randomMappings),
                                ot2.aliases()
                            ),
                            orig.version(),
                            orig.metadata()
                        );
                    }
                    case 2 -> {
                        Template ot3 = orig.template();
                        return new ComponentTemplate(
                            new Template(
                                ot3.settings(),
                                ot3.mappings(),
                                randomValueOtherThan(ot3.aliases(), ComponentTemplateTests::randomAliases)
                            ),
                            orig.version(),
                            orig.metadata()
                        );
                    }
                    default -> throw new IllegalStateException("illegal randomization branch");
                }
            case 1:
                return new ComponentTemplate(
                    orig.template(),
                    randomValueOtherThan(orig.version(), ESTestCase::randomNonNegativeLong),
                    orig.metadata()
                );
            case 2:
                return new ComponentTemplate(
                    orig.template(),
                    orig.version(),
                    randomValueOtherThan(orig.metadata(), ComponentTemplateTests::randomMeta)
                );
            default:
                throw new IllegalStateException("illegal randomization branch");
        }
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
            CompressedXContent m1 = new CompressedXContent("""
                {"properties":{"%s":{"type":"keyword"}}}
                """.formatted(randomString));
            CompressedXContent m2 = new CompressedXContent("""
                {"properties":{"%s":{"type":"keyword"}}}
                """.formatted(randomString));
            assertThat(Template.mappingsEquals(m1, m2), equalTo(true));
        }

        {
            CompressedXContent m1 = randomMappings();
            CompressedXContent m2 = new CompressedXContent("""
                {"properties":{"%s":{"type":"keyword"}}}
                """.formatted(randomAlphaOfLength(10)));
            assertThat(Template.mappingsEquals(m1, m2), equalTo(false));
        }

        {
            Map<String, Object> map = XContentHelper.convertToMap(new BytesArray("""
                {"%s":{"properties":{"%s":{"type":"keyword"}}}}
                """.formatted(MapperService.SINGLE_MAPPING_NAME, randomAlphaOfLength(10))), true, XContentType.JSON).v2();
            Map<String, Object> reduceMap = Template.reduceMapping(map);
            CompressedXContent m1 = new CompressedXContent(Strings.toString(XContentFactory.jsonBuilder().map(map)));
            CompressedXContent m2 = new CompressedXContent(Strings.toString(XContentFactory.jsonBuilder().map(reduceMap)));
            assertThat(Template.mappingsEquals(m1, m2), equalTo(true));
        }
    }
}
