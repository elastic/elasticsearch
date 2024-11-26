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

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class LifecyclePolicyUtilsTests extends ESTestCase {
    public void testCalculateUsage() {
        final IndexNameExpressionResolver iner = TestIndexNameExpressionResolver.newInstance();

        {
            // Test where policy does not exist
            var project = ProjectMetadata.builder(randomProjectId()).build();
            assertThat(
                LifecyclePolicyUtils.calculateUsage(iner, project, "mypolicy"),
                equalTo(new ItemUsage(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()))
            );
        }

        {
            // Test where policy is not used by anything
            var project = ProjectMetadata.builder(randomProjectId())
                .putCustom(
                    IndexLifecycleMetadata.TYPE,
                    new IndexLifecycleMetadata(
                        Collections.singletonMap("mypolicy", LifecyclePolicyMetadataTests.createRandomPolicyMetadata("mypolicy")),
                        OperationMode.RUNNING
                    )
                )
                .build();
            assertThat(
                LifecyclePolicyUtils.calculateUsage(iner, project, "mypolicy"),
                equalTo(new ItemUsage(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()))
            );
        }

        {
            // Test where policy exists and is used by an index
            var project = ProjectMetadata.builder(randomProjectId())
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
                .build();
            assertThat(
                LifecyclePolicyUtils.calculateUsage(iner, project, "mypolicy"),
                equalTo(new ItemUsage(Collections.singleton("myindex"), Collections.emptyList(), Collections.emptyList()))
            );
        }

        {
            // Test where policy exists and is used by an index, and template
            var project = ProjectMetadata.builder(randomProjectId())
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
                equalTo(new ItemUsage(Collections.singleton("myindex"), Collections.emptyList(), Collections.singleton("mytemplate")))
            );
        }

        {
            var projectBuilder = ProjectMetadata.builder(randomProjectId())
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
            projectBuilder.put(
                DataStreamTestHelper.newInstance("myds", Collections.singletonList(projectBuilder.get("myindex").getIndex()))
            );

            // Test where policy exists and is used by an index, datastream, and template
            assertThat(
                LifecyclePolicyUtils.calculateUsage(iner, projectBuilder.build(), "mypolicy"),
                equalTo(
                    new ItemUsage(Arrays.asList("myindex", "another"), Collections.singleton("myds"), Collections.singleton("mytemplate"))
                )
            );
        }
    }
}
