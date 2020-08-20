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
import java.util.List;
import java.util.Map;

public class ComposableIndexTemplateTests extends AbstractDiffableSerializationTestCase<ComposableIndexTemplate> {
    @Override
    protected ComposableIndexTemplate makeTestChanges(ComposableIndexTemplate testInstance) {
        try {
            return mutateInstance(testInstance);
        } catch (IOException e) {
            logger.error(e);
            fail("mutating should not throw an exception, but got: " + e);
            return null;
        }
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
        Settings settings = null;
        CompressedXContent mappings = null;
        Map<String, AliasMetadata> aliases = null;
        Template template = null;
        ComposableIndexTemplate.DataStreamTemplate dataStreamTemplate = randomDataStreamTemplate();

        if (dataStreamTemplate != null || randomBoolean()) {
            if (randomBoolean()) {
                settings = randomSettings();
            }
            if (dataStreamTemplate != null || randomBoolean()) {
                mappings = randomMappings(dataStreamTemplate);
            }
            if (randomBoolean()) {
                aliases = randomAliases();
            }
            template = new Template(settings, mappings, aliases);
        }

        Map<String, Object> meta = null;
        if (randomBoolean()) {
            meta = randomMeta();
        }

        List<String> indexPatterns = randomList(1, 4, () -> randomAlphaOfLength(4));
        List<String> componentTemplates = randomList(0, 10, () -> randomAlphaOfLength(5));
        return new ComposableIndexTemplate(indexPatterns,
            template,
            componentTemplates,
            randomBoolean() ? null : randomNonNegativeLong(),
            randomBoolean() ? null : randomNonNegativeLong(),
            meta,
            dataStreamTemplate);
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
                return new CompressedXContent("{\"properties\":{\"" + dataStreamTemplate.getTimestampField() + "\":{\"type\":\"date\"}}}");
            } else {
                return new CompressedXContent("{\"properties\":{\"" + randomAlphaOfLength(5) + "\":{\"type\":\"keyword\"}}}");
            }
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

    private static ComposableIndexTemplate.DataStreamTemplate randomDataStreamTemplate() {
        if (randomBoolean()) {
            return null;
        } else {
            return new ComposableIndexTemplate.DataStreamTemplate();
        }
    }

    @Override
    protected ComposableIndexTemplate mutateInstance(ComposableIndexTemplate orig) throws IOException {
        return mutateTemplate(orig);
    }

    public static ComposableIndexTemplate mutateTemplate(ComposableIndexTemplate orig) {
        switch (randomIntBetween(0, 6)) {
            case 0:
                List<String> newIndexPatterns = randomValueOtherThan(orig.indexPatterns(),
                    () -> randomList(1, 4, () -> randomAlphaOfLength(4)));
                return new ComposableIndexTemplate(newIndexPatterns, orig.template(), orig.composedOf(),
                    orig.priority(), orig.version(), orig.metadata(), orig.getDataStreamTemplate());
            case 1:
                return new ComposableIndexTemplate(orig.indexPatterns(),
                    randomValueOtherThan(orig.template(), () -> new Template(randomSettings(),
                        randomMappings(orig.getDataStreamTemplate()), randomAliases())),
                    orig.composedOf(),
                    orig.priority(),
                    orig.version(),
                    orig.metadata(),
                    orig.getDataStreamTemplate());
            case 2:
                List<String> newComposedOf = randomValueOtherThan(orig.composedOf(),
                    () -> randomList(0, 10, () -> randomAlphaOfLength(5)));
                return new ComposableIndexTemplate(orig.indexPatterns(),
                    orig.template(),
                    newComposedOf,
                    orig.priority(),
                    orig.version(),
                    orig.metadata(),
                    orig.getDataStreamTemplate());
            case 3:
                return new ComposableIndexTemplate(orig.indexPatterns(),
                    orig.template(),
                    orig.composedOf(),
                    randomValueOtherThan(orig.priority(), ESTestCase::randomNonNegativeLong),
                    orig.version(),
                    orig.metadata(),
                    orig.getDataStreamTemplate());
            case 4:
                return new ComposableIndexTemplate(orig.indexPatterns(),
                    orig.template(),
                    orig.composedOf(),
                    orig.priority(),
                    randomValueOtherThan(orig.version(), ESTestCase::randomNonNegativeLong),
                    orig.metadata(),
                    orig.getDataStreamTemplate());
            case 5:
                return new ComposableIndexTemplate(orig.indexPatterns(),
                    orig.template(),
                    orig.composedOf(),
                    orig.priority(),
                    orig.version(),
                    randomValueOtherThan(orig.metadata(), ComposableIndexTemplateTests::randomMeta),
                    orig.getDataStreamTemplate());
            case 6:
                return new ComposableIndexTemplate(orig.indexPatterns(),
                    orig.template(),
                    orig.composedOf(),
                    orig.priority(),
                    orig.version(),
                    orig.metadata(),
                    randomValueOtherThan(orig.getDataStreamTemplate(), ComposableIndexTemplateTests::randomDataStreamTemplate));
            default:
                throw new IllegalStateException("illegal randomization branch");
        }
    }
}
