/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ItemUsage;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class LifecyclePolicyUsageCalculatorTests extends ESTestCase {

    private IndexNameExpressionResolver iner;

    @Before
    public void init() {
        iner = new IndexNameExpressionResolver(
            new ThreadContext(Settings.EMPTY),
            EmptySystemIndices.INSTANCE,
            TestProjectResolvers.alwaysThrow()
        );
    }

    public void testGetUsageNonExistentPolicy() {
        // Test where policy does not exist
        final var project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        assertThat(
            new LifecyclePolicyUsageCalculator(iner, project, List.of("mypolicy")).retrieveCalculatedUsage("mypolicy"),
            equalTo(new ItemUsage(List.of(), List.of(), List.of()))
        );
    }

    public void testGetUsageUnusedPolicy() {
        // Test where policy is not used by anything
        final var project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .putCustom(
                IndexLifecycleMetadata.TYPE,
                new IndexLifecycleMetadata(
                    Map.of("mypolicy", LifecyclePolicyMetadataTests.createRandomPolicyMetadata("mypolicy")),
                    OperationMode.RUNNING
                )
            )
            .build();

        assertThat(
            new LifecyclePolicyUsageCalculator(iner, project, List.of("mypolicy")).retrieveCalculatedUsage("mypolicy"),
            equalTo(new ItemUsage(List.of(), List.of(), List.of()))
        );
    }

    public void testGetUsagePolicyUsedByIndex() {
        // Test where policy exists and is used by an index
        final var project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .putCustom(
                IndexLifecycleMetadata.TYPE,
                new IndexLifecycleMetadata(
                    Map.of("mypolicy", LifecyclePolicyMetadataTests.createRandomPolicyMetadata("mypolicy")),
                    OperationMode.RUNNING
                )
            )
            .put(
                IndexMetadata.builder("myindex")
                    .settings(indexSettings(IndexVersion.current(), 1, 0).put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy"))
            )
            .build();

        assertThat(
            new LifecyclePolicyUsageCalculator(iner, project, List.of("mypolicy")).retrieveCalculatedUsage("mypolicy"),
            equalTo(new ItemUsage(List.of("myindex"), List.of(), List.of()))
        );
    }

    public void testGetUsagePolicyUsedByIndexAndTemplate() {
        // Test where policy exists and is used by an index, and template
        final var project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .putCustom(
                IndexLifecycleMetadata.TYPE,
                new IndexLifecycleMetadata(
                    Map.of("mypolicy", LifecyclePolicyMetadataTests.createRandomPolicyMetadata("mypolicy")),
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
                    Map.of(
                        "mytemplate",
                        ComposableIndexTemplate.builder()
                            .indexPatterns(List.of("myds"))
                            .template(
                                new Template(Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy").build(), null, null)
                            )
                            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                            .build()
                    )
                )
            )
            .build();

        assertThat(
            new LifecyclePolicyUsageCalculator(iner, project, List.of("mypolicy")).retrieveCalculatedUsage("mypolicy"),
            equalTo(new ItemUsage(List.of("myindex"), List.of(), List.of("mytemplate")))
        );
    }

    public void testGetUsagePolicyUsedByIndexAndTemplateAndDataStream() {
        // Test where policy exists and is used by an index, data stream, and template
        IndexMetadata index = IndexMetadata.builder("myindex")
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy"))
            .build();
        final var project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .putCustom(
                IndexLifecycleMetadata.TYPE,
                new IndexLifecycleMetadata(
                    Map.of("mypolicy", LifecyclePolicyMetadataTests.createRandomPolicyMetadata("mypolicy")),
                    OperationMode.RUNNING
                )
            )
            .put(index, false)
            .put(DataStreamTestHelper.newInstance("myds", List.of(index.getIndex())))
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
                    Map.of(
                        "mytemplate",
                        ComposableIndexTemplate.builder()
                            .indexPatterns(List.of("myds"))
                            .template(
                                new Template(Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy").build(), null, null)
                            )
                            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                            .build()
                    )
                )
            )
            .build();

        assertThat(
            new LifecyclePolicyUsageCalculator(iner, project, List.of("mypolicy")).retrieveCalculatedUsage("mypolicy"),
            equalTo(new ItemUsage(List.of("myindex", "another"), List.of("myds"), List.of("mytemplate")))
        );
    }

    public void testGetUsagePolicyNotUsedByDataStreamDueToOverride() {
        // Test when a data stream does not use the policy anymore because of a higher template
        IndexMetadata index = IndexMetadata.builder("myindex")
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy"))
            .build();
        final var project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(index, false)
            .put(DataStreamTestHelper.newInstance("myds", List.of(index.getIndex())))
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
                            .indexPatterns(List.of("myds*"))
                            .template(
                                new Template(Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy").build(), null, null)
                            )
                            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                            .build(),
                        "myhighertemplate",
                        ComposableIndexTemplate.builder()
                            .indexPatterns(List.of("myds"))
                            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                            .priority(1_000L)
                            .build()
                    )
                )
            )
            .build();

        // Test where policy exists and is used by an index, datastream, and template
        assertThat(
            new LifecyclePolicyUsageCalculator(iner, project, List.of("mypolicy")).retrieveCalculatedUsage("mypolicy"),
            equalTo(new ItemUsage(List.of("myindex"), List.of(), List.of("mytemplate")))
        );
    }
}
