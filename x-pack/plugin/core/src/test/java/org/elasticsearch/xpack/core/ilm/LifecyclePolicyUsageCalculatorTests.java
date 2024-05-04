/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ItemUsage;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class LifecyclePolicyUsageCalculatorTests extends ESTestCase {

    private IndexNameExpressionResolver iner;

    @Before
    public void init() {
        iner = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY), EmptySystemIndices.INSTANCE);
    }

    public void testGetUsageNonExistentPolicy() {
        // Test where policy does not exist
        ClusterState state = ClusterState.builder(new ClusterName("mycluster")).build();
        assertThat(
            new LifecyclePolicyUsageCalculator(iner, state, List.of("mypolicy")).calculateUsage("mypolicy"),
            equalTo(new ItemUsage(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()))
        );
    }

    public void testGetUsageUnusedPolicy() {
        // Test where policy is not used by anything
        ClusterState state = ClusterState.builder(new ClusterName("mycluster"))
            .metadata(
                Metadata.builder()
                    .putCustom(
                        IndexLifecycleMetadata.TYPE,
                        new IndexLifecycleMetadata(
                            Collections.singletonMap("mypolicy", LifecyclePolicyMetadataTests.createRandomPolicyMetadata("mypolicy")),
                            OperationMode.RUNNING
                        )
                    )
                    .build()
            )
            .build();
        assertThat(
            new LifecyclePolicyUsageCalculator(iner, state, List.of("mypolicy")).calculateUsage("mypolicy"),
            equalTo(new ItemUsage(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()))
        );
    }

    public void testGetUsagePolicyUsedByIndex() {

        // Test where policy exists and is used by an index
        ClusterState state = ClusterState.builder(new ClusterName("mycluster"))
            .metadata(
                Metadata.builder()
                    .putCustom(
                        IndexLifecycleMetadata.TYPE,
                        new IndexLifecycleMetadata(
                            Collections.singletonMap("mypolicy", LifecyclePolicyMetadataTests.createRandomPolicyMetadata("mypolicy")),
                            OperationMode.RUNNING
                        )
                    )
                    .put(
                        IndexMetadata.builder("myindex")
                            .settings(indexSettings(IndexVersion.current(), 1, 0).put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy"))
                    )
                    .build()
            )
            .build();
        assertThat(
            new LifecyclePolicyUsageCalculator(iner, state, List.of("mypolicy")).calculateUsage("mypolicy"),
            equalTo(new ItemUsage(Collections.singleton("myindex"), Collections.emptyList(), Collections.emptyList()))
        );
    }

    public void testGetUsagePolicyUsedByIndexAndTemplate() {

        // Test where policy exists and is used by an index, and template
        ClusterState state = ClusterState.builder(new ClusterName("mycluster"))
            .metadata(
                Metadata.builder()
                    .putCustom(
                        IndexLifecycleMetadata.TYPE,
                        new IndexLifecycleMetadata(
                            Collections.singletonMap("mypolicy", LifecyclePolicyMetadataTests.createRandomPolicyMetadata("mypolicy")),
                            OperationMode.RUNNING
                        )
                    )
                    .put(
                        IndexMetadata.builder("myindex")
                            .settings(indexSettings(IndexVersion.current(), 1, 0).put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy"))
                    )
                    .putCustom(
                        ComposableIndexTemplateMetadata.TYPE,
                        new ComposableIndexTemplateMetadata(
                            Collections.singletonMap(
                                "mytemplate",
                                ComposableIndexTemplate.builder()
                                    .indexPatterns(Collections.singletonList("myds"))
                                    .template(
                                        new Template(
                                            Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy").build(),
                                            null,
                                            null
                                        )
                                    )
                                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                                    .build()
                            )
                        )
                    )
                    .build()
            )
            .build();
        assertThat(
            new LifecyclePolicyUsageCalculator(iner, state, List.of("mypolicy")).calculateUsage("mypolicy"),
            equalTo(new ItemUsage(Collections.singleton("myindex"), Collections.emptyList(), Collections.singleton("mytemplate")))
        );
    }

    public void testGetUsagePolicyUsedByIndexAndTemplateAndDataStream() {
        // Test where policy exists and is used by an index, data stream, and template
        Metadata.Builder mBuilder = Metadata.builder()
            .putCustom(
                IndexLifecycleMetadata.TYPE,
                new IndexLifecycleMetadata(
                    Collections.singletonMap("mypolicy", LifecyclePolicyMetadataTests.createRandomPolicyMetadata("mypolicy")),
                    OperationMode.RUNNING
                )
            )
            .put(
                IndexMetadata.builder("myindex")
                    .settings(indexSettings(IndexVersion.current(), 1, 0).put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy"))
            )
            .put(
                IndexMetadata.builder("another")
                    .settings(indexSettings(IndexVersion.current(), 1, 0).put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy"))
            )
            .put(
                IndexMetadata.builder("other")
                    .settings(indexSettings(IndexVersion.current(), 1, 0).put(LifecycleSettings.LIFECYCLE_NAME, "otherpolicy"))
            )

            .putCustom(
                ComposableIndexTemplateMetadata.TYPE,
                new ComposableIndexTemplateMetadata(
                    Collections.singletonMap(
                        "mytemplate",
                        ComposableIndexTemplate.builder()
                            .indexPatterns(Collections.singletonList("myds"))
                            .template(
                                new Template(Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy").build(), null, null)
                            )
                            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                            .build()
                    )
                )
            );
        // Need to get the real Index instance of myindex:
        mBuilder.put(DataStreamTestHelper.newInstance("myds", Collections.singletonList(mBuilder.get("myindex").getIndex())));

        ClusterState state = ClusterState.builder(new ClusterName("mycluster")).metadata(mBuilder.build()).build();
        assertThat(
            new LifecyclePolicyUsageCalculator(iner, state, List.of("mypolicy")).calculateUsage("mypolicy"),
            equalTo(new ItemUsage(Arrays.asList("myindex", "another"), Collections.singleton("myds"), Collections.singleton("mytemplate")))
        );
    }

    public void testGetUsagePolicyNotUsedByDataStreamDueToOverride() {
        // Test when a data stream does not use the policy anymore because of a higher template
        Metadata.Builder mBuilder = Metadata.builder()
            .put(
                IndexMetadata.builder("myindex")
                    .settings(indexSettings(IndexVersion.current(), 1, 0).put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy"))
            )
            .putCustom(
                IndexLifecycleMetadata.TYPE,
                new IndexLifecycleMetadata(
                    Map.of("mypolicy", LifecyclePolicyMetadataTests.createRandomPolicyMetadata("mypolicy")),
                    OperationMode.RUNNING
                )
            )
            .putCustom(
                ComposableIndexTemplateMetadata.TYPE,
                new ComposableIndexTemplateMetadata(
                    Map.of(
                        "mytemplate",
                        ComposableIndexTemplate.builder()
                            .indexPatterns(Collections.singletonList("myds*"))
                            .template(
                                new Template(Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy").build(), null, null)
                            )
                            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                            .build(),
                        "myhighertemplate",
                        ComposableIndexTemplate.builder()
                            .indexPatterns(Collections.singletonList("myds"))
                            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                            .priority(1_000L)
                            .build()
                    )
                )
            );

        mBuilder.put(DataStreamTestHelper.newInstance("myds", Collections.singletonList(mBuilder.get("myindex").getIndex())));

        // Test where policy exists and is used by an index, datastream, and template
        ClusterState state = ClusterState.builder(new ClusterName("mycluster")).metadata(mBuilder.build()).build();
        assertThat(
            new LifecyclePolicyUsageCalculator(iner, state, List.of("mypolicy")).calculateUsage("mypolicy"),
            equalTo(new ItemUsage(List.of("myindex"), List.of(), List.of("mytemplate")))
        );
    }
}
