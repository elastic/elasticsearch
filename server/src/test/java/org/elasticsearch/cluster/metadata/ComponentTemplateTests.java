/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractDiffableSerializationTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class ComponentTemplateTests extends AbstractDiffableSerializationTestCase<ComponentTemplate> {
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
            return Collections.singletonMap(randomAlphaOfLength(5),
                Collections.singletonMap(randomAlphaOfLength(4), randomAlphaOfLength(4)));
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
                    case 0:
                        Template ot = orig.template();
                        return new ComponentTemplate(
                            new Template(randomValueOtherThan(ot.settings(), ComponentTemplateTests::randomSettings),
                                ot.mappings(), ot.aliases()),
                            orig.version(), orig.metadata());
                    case 1:
                        Template ot2 = orig.template();
                        return new ComponentTemplate(
                            new Template(ot2.settings(),
                                randomValueOtherThan(ot2.mappings(), ComponentTemplateTests::randomMappings), ot2.aliases()),
                            orig.version(), orig.metadata());
                    case 2:
                        Template ot3 = orig.template();
                        return new ComponentTemplate(
                            new Template(ot3.settings(), ot3.mappings(),
                                randomValueOtherThan(ot3.aliases(), ComponentTemplateTests::randomAliases)),
                            orig.version(), orig.metadata());
                    default:
                        throw new IllegalStateException("illegal randomization branch");
                }
            case 1:
                return new ComponentTemplate(orig.template(), randomValueOtherThan(orig.version(), ESTestCase::randomNonNegativeLong),
                    orig.metadata());
            case 2:
                return new ComponentTemplate(orig.template(), orig.version(),
                    randomValueOtherThan(orig.metadata(), ComponentTemplateTests::randomMeta));
            default:
                throw new IllegalStateException("illegal randomization branch");
        }
    }
}
