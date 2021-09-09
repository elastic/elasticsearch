/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.client.transform.TransformNamedXContentProvider;
import org.elasticsearch.client.transform.transforms.latest.LatestConfig;
import org.elasticsearch.client.transform.transforms.latest.LatestConfigTests;
import org.elasticsearch.client.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.client.transform.transforms.pivot.PivotConfigTests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.client.transform.transforms.DestConfigTests.randomDestConfig;
import static org.elasticsearch.client.transform.transforms.SourceConfigTests.randomSourceConfig;

public class TransformConfigTests extends AbstractXContentTestCase<TransformConfig> {

    public static TransformConfig randomTransformConfig() {
        PivotConfig pivotConfig;
        LatestConfig latestConfig;
        if (randomBoolean()) {
            pivotConfig = PivotConfigTests.randomPivotConfig();
            latestConfig = null;
        } else {
            pivotConfig = null;
            latestConfig = LatestConfigTests.randomLatestConfig();
        }
        return new TransformConfig(
            randomAlphaOfLengthBetween(1, 10),
            randomSourceConfig(),
            randomDestConfig(),
            randomBoolean() ? null : TimeValue.timeValueMillis(randomIntBetween(1000, 1000000)),
            randomBoolean() ? null : randomSyncConfig(),
            pivotConfig,
            latestConfig,
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 100),
            SettingsConfigTests.randomSettingsConfig(),
            randomBoolean() ? null : randomRetentionPolicyConfig(),
            randomBoolean() ? null : Instant.now(),
            randomBoolean() ? null : Version.CURRENT.toString()
        );
    }

    public static SyncConfig randomSyncConfig() {
        return TimeSyncConfigTests.randomTimeSyncConfig();
    }

    public static RetentionPolicyConfig randomRetentionPolicyConfig() {
        return TimeRetentionPolicyConfigTests.randomTimeRetentionPolicyConfig();
    }

    @Override
    protected TransformConfig createTestInstance() {
        return randomTransformConfig();
    }

    @Override
    protected TransformConfig doParseInstance(XContentParser parser) throws IOException {
        return TransformConfig.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // allow unknown fields in the root of the object only
        return field -> field.isEmpty() == false;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        List<NamedXContentRegistry.Entry> namedXContents = searchModule.getNamedXContents();
        namedXContents.addAll(new TransformNamedXContentProvider().getNamedXContentParsers());

        return new NamedXContentRegistry(namedXContents);
    }
}
