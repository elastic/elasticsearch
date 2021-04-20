/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.AbstractSerializingTransformTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonMap;

public class TransformDestIndexSettingsTests extends AbstractSerializingTransformTestCase<TransformDestIndexSettings> {

    public static TransformDestIndexSettings randomDestIndexSettings() {
        int size = randomIntBetween(0, 10);

        Map<String, Object> mappings = null;

        if (randomBoolean()) {
            mappings = new HashMap<>(size);
            mappings.put("_meta", singletonMap("_transform", singletonMap("version", Version.CURRENT.toString())));
            for (int i = 0; i < size; i++) {
                mappings.put(randomAlphaOfLength(10), singletonMap("type", randomAlphaOfLength(10)));
            }
        }

        Settings settings = null;
        if (randomBoolean()) {
            Settings.Builder settingsBuilder = Settings.builder();
            size = randomIntBetween(0, 10);
            for (int i = 0; i < size; i++) {
                settingsBuilder.put(randomAlphaOfLength(10), randomBoolean());
            }
            settings = settingsBuilder.build();
        }

        Set<Alias> aliases = null;

        if (randomBoolean()) {
            aliases = new HashSet<>();
            size = randomIntBetween(0, 10);
            for (int i = 0; i < size; i++) {
                aliases.add(new Alias(randomAlphaOfLength(10)));
            }
        }

        return new TransformDestIndexSettings(mappings, settings, aliases);
    }

    @Override
    protected TransformDestIndexSettings doParseInstance(XContentParser parser) throws IOException {
        return TransformDestIndexSettings.fromXContent(parser);
    }

    @Override
    protected Reader<TransformDestIndexSettings> instanceReader() {
        return TransformDestIndexSettings::new;
    }

    @Override
    protected TransformDestIndexSettings createTestInstance() {
        return randomDestIndexSettings();
    }
}
