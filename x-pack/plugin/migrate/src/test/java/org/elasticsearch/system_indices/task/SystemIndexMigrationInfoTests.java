/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.task;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptorUtils;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class SystemIndexMigrationInfoTests extends ESTestCase {

    private static final String TEST_INDEX = ".test-index";
    private static final String TEST_FEATURE_NAME = "test-feature";

    public void testBuildWithUnmanagedIndexWithMapping() {
        String mappingSource = """
            {"_doc":{"properties":{"field":{"type":"keyword"}}}}""";
        IndexMetadata indexMetadata = IndexMetadata.builder(TEST_INDEX)
            .settings(Settings.builder().put("index.version.created", IndexVersion.current()).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .system(true)
            .putMapping(mappingSource)
            .build();

        SystemIndexDescriptor descriptor = SystemIndexDescriptorUtils.createUnmanaged(TEST_INDEX + "*", "test descriptor");
        SystemIndices.Feature feature = new SystemIndices.Feature(TEST_FEATURE_NAME, "test feature", List.of(descriptor));

        SystemIndexMigrationInfo info = SystemIndexMigrationInfo.build(
            indexMetadata,
            descriptor,
            feature,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS
        );

        assertThat(info.getMappings(), equalTo(mappingSource));
        assertThat(info.getCurrentIndexName(), equalTo(TEST_INDEX));
    }

    public void testBuildWithUnmanagedIndexWithoutMapping() {
        IndexMetadata indexMetadata = IndexMetadata.builder(TEST_INDEX)
            .settings(Settings.builder().put("index.version.created", IndexVersion.current()).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .system(true)
            .build();

        SystemIndexDescriptor descriptor = SystemIndexDescriptorUtils.createUnmanaged(TEST_INDEX + "*", "test descriptor");
        SystemIndices.Feature feature = new SystemIndices.Feature(TEST_FEATURE_NAME, "test feature", List.of(descriptor));

        SystemIndexMigrationInfo info = SystemIndexMigrationInfo.build(
            indexMetadata,
            descriptor,
            feature,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS
        );

        assertThat(info.getMappings(), nullValue());
        assertThat(info.getCurrentIndexName(), equalTo(TEST_INDEX));
    }
}
