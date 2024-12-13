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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class LifecyclePolicyUtilsTests extends ESTestCase {
    public void testCalculateUsage() {
        final IndexNameExpressionResolver iner = TestIndexNameExpressionResolver.newInstance();

        {
            // Test where policy does not exist
            var project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
            assertThat(
                LifecyclePolicyUtils.calculateUsage(iner, project, "mypolicy"),
                equalTo(new ItemUsage(List.of(), List.of(), List.of()))
            );
        }

        {
            // Test where policy is not used by anything
            var project = ProjectMetadata.builder(randomProjectIdOrDefault())
                .putCustom(
                    IndexLifecycleMetadata.TYPE,
                    new IndexLifecycleMetadata(
                        Map.of("mypolicy", LifecyclePolicyMetadataTests.createRandomPolicyMetadata("mypolicy")),
                        OperationMode.RUNNING
                    )
                )
                .build();
            assertThat(
                LifecyclePolicyUtils.calculateUsage(iner, project, "mypolicy"),
                equalTo(new ItemUsage(List.of(), List.of(), List.of()))
            );
        }

        {
            // Test where policy exists and is used by an index
            var project = ProjectMetadata.builder(randomProjectIdOrDefault())
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
                LifecyclePolicyUtils.calculateUsage(iner, project, "mypolicy"),
                equalTo(new ItemUsage(List.of("myindex"), List.of(), List.of()))
            );
        }

        {
            // Test where policy exists and is used by an index, and template
            var project = ProjectMetadata.builder(randomProjectIdOrDefault())
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
                LifecyclePolicyUtils.calculateUsage(iner, project, "mypolicy"),
                equalTo(new ItemUsage(List.of("myindex"), List.of(), List.of("mytemplate")))
            );
        }

        {
            var projectBuilder = ProjectMetadata.builder(randomProjectIdOrDefault())
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
                );
            // Need to get the real Index instance of myindex:
            projectBuilder.put(DataStreamTestHelper.newInstance("myds", List.of(projectBuilder.get("myindex").getIndex())));

            // Test where policy exists and is used by an index, datastream, and template
            assertThat(
                LifecyclePolicyUtils.calculateUsage(iner, projectBuilder.build(), "mypolicy"),
                equalTo(new ItemUsage(List.of("myindex", "another"), List.of("myds"), List.of("mytemplate")))
            );
        }
    }
}
