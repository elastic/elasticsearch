/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ItemUsage;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class LifecyclePolicyUtilsTests extends ESTestCase {
    public void testCalculateUsage() {
        final IndexNameExpressionResolver iner =
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY), EmptySystemIndices.INSTANCE);

        {
            // Test where policy does not exist
            ClusterState state = ClusterState.builder(new ClusterName("mycluster")).build();
            assertThat(LifecyclePolicyUtils.calculateUsage(iner, state, "mypolicy"),
                equalTo(new ItemUsage(Collections.emptyList(), Collections.emptyList(), Collections.emptyList())));
        }

        {
            // Test where policy is not used by anything
            ClusterState state = ClusterState.builder(new ClusterName("mycluster"))
                .metadata(Metadata.builder()
                    .putCustom(IndexLifecycleMetadata.TYPE,
                        new IndexLifecycleMetadata(Collections.singletonMap("mypolicy",
                            LifecyclePolicyMetadataTests.createRandomPolicyMetadata("mypolicy")), OperationMode.RUNNING))
                    .build())
                .build();
            assertThat(LifecyclePolicyUtils.calculateUsage(iner, state, "mypolicy"),
                equalTo(new ItemUsage(Collections.emptyList(), Collections.emptyList(), Collections.emptyList())));
        }

        {
            // Test where policy exists and is used by an index
            ClusterState state = ClusterState.builder(new ClusterName("mycluster"))
                .metadata(Metadata.builder()
                    .putCustom(IndexLifecycleMetadata.TYPE,
                        new IndexLifecycleMetadata(Collections.singletonMap("mypolicy",
                            LifecyclePolicyMetadataTests.createRandomPolicyMetadata("mypolicy")), OperationMode.RUNNING))
                    .put(IndexMetadata.builder("myindex")
                    .settings(Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy")
                        .build()))
                    .build())
                .build();
            assertThat(LifecyclePolicyUtils.calculateUsage(iner, state, "mypolicy"),
                equalTo(new ItemUsage(Collections.singleton("myindex"), Collections.emptyList(), Collections.emptyList())));
        }

        {
            // Test where policy exists and is used by an index, and template
            ClusterState state = ClusterState.builder(new ClusterName("mycluster"))
                .metadata(Metadata.builder()
                    .putCustom(IndexLifecycleMetadata.TYPE,
                        new IndexLifecycleMetadata(Collections.singletonMap("mypolicy",
                            LifecyclePolicyMetadataTests.createRandomPolicyMetadata("mypolicy")), OperationMode.RUNNING))
                    .put(IndexMetadata.builder("myindex")
                        .settings(Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy")
                            .build()))
                    .putCustom(ComposableIndexTemplateMetadata.TYPE,
                        new ComposableIndexTemplateMetadata(Collections.singletonMap("mytemplate",
                            new ComposableIndexTemplate(Collections.singletonList("myds"),
                                new Template(Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy").build(), null, null),
                                null, null, null, null, new ComposableIndexTemplate.DataStreamTemplate(false)))))
                    .build())
                .build();
            assertThat(LifecyclePolicyUtils.calculateUsage(iner, state, "mypolicy"),
                equalTo(new ItemUsage(Collections.singleton("myindex"), Collections.emptyList(), Collections.singleton("mytemplate"))));
        }

        {
            // Test where policy exists and is used by an index, datastream, and template
            ClusterState state = ClusterState.builder(new ClusterName("mycluster"))
                .metadata(Metadata.builder()
                    .putCustom(IndexLifecycleMetadata.TYPE,
                        new IndexLifecycleMetadata(Collections.singletonMap("mypolicy",
                            LifecyclePolicyMetadataTests.createRandomPolicyMetadata("mypolicy")), OperationMode.RUNNING))
                    .put(IndexMetadata.builder("myindex")
                        .settings(Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy")
                            .build()))
                    .put(IndexMetadata.builder("another")
                        .settings(Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy")
                            .build()))
                    .put(IndexMetadata.builder("other")
                        .settings(Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(LifecycleSettings.LIFECYCLE_NAME, "otherpolicy")
                            .build()))
                    .put(new DataStream("myds", new DataStream.TimestampField("@timestamp"),
                        Collections.singletonList(new Index("myindex", "uuid"))))
                    .putCustom(ComposableIndexTemplateMetadata.TYPE,
                        new ComposableIndexTemplateMetadata(Collections.singletonMap("mytemplate",
                            new ComposableIndexTemplate(Collections.singletonList("myds"),
                                new Template(Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy").build(), null, null),
                                null, null, null, null, new ComposableIndexTemplate.DataStreamTemplate(false)))))
                    .build())
                .build();
            assertThat(LifecyclePolicyUtils.calculateUsage(iner, state, "mypolicy"),
                equalTo(new ItemUsage(Arrays.asList("myindex", "another"),
                    Collections.singleton("myds"),
                    Collections.singleton("mytemplate"))));
        }
    }
}
