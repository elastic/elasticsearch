/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.client.transform.TransformNamedXContentProvider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.client.transform.transforms.DestConfigTests.randomDestConfig;
import static org.elasticsearch.client.transform.transforms.SettingsConfigTests.randomSettingsConfig;
import static org.elasticsearch.client.transform.transforms.SourceConfigTests.randomSourceConfig;
import static org.elasticsearch.client.transform.transforms.TransformConfigTests.randomRetentionPolicyConfig;
import static org.elasticsearch.client.transform.transforms.TransformConfigTests.randomSyncConfig;

public class TransformConfigUpdateTests extends AbstractXContentTestCase<TransformConfigUpdate> {

    public static TransformConfigUpdate randomTransformConfigUpdate() {
        return new TransformConfigUpdate(
            randomBoolean() ? null : randomSourceConfig(),
            randomBoolean() ? null : randomDestConfig(),
            randomBoolean() ? null : TimeValue.timeValueMillis(randomIntBetween(1_000, 3_600_000)),
            randomBoolean() ? null : randomSyncConfig(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            randomBoolean() ? null : randomSettingsConfig(),
            randomBoolean() ? null : randomRetentionPolicyConfig()
        );
    }

    @Override
    protected TransformConfigUpdate doParseInstance(XContentParser parser) throws IOException {
        return TransformConfigUpdate.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected TransformConfigUpdate createTestInstance() {
        return randomTransformConfigUpdate();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        List<NamedXContentRegistry.Entry> namedXContents = searchModule.getNamedXContents();
        namedXContents.addAll(new TransformNamedXContentProvider().getNamedXContentParsers());

        return new NamedXContentRegistry(namedXContents);
    }
}
