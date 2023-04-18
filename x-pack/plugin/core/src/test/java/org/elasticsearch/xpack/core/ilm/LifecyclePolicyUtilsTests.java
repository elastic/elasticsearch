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
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ItemUsage;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class LifecyclePolicyUtilsTests extends ESTestCase {
    public void testCalculateUsage() {
        final IndexNameExpressionResolver iner = new IndexNameExpressionResolver(
            new ThreadContext(Settings.EMPTY),
            EmptySystemIndices.INSTANCE
        );

        {
            // Test where policy does not exist
            ClusterState state = ClusterState.builder(new ClusterName("mycluster")).build();
            assertThat(
                LifecyclePolicyUtils.calculateUsage(iner, state, "mypolicy"),
                equalTo(new ItemUsage(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()))
            );
        }

        {
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
                LifecyclePolicyUtils.calculateUsage(iner, state, "mypolicy"),
                equalTo(new ItemUsage(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()))
            );
        }

        {
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
                                .settings(indexSettings(Version.CURRENT, 1, 0).put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy"))
                        )
                        .build()
                )
                .build();
            assertThat(
                LifecyclePolicyUtils.calculateUsage(iner, state, "mypolicy"),
                equalTo(new ItemUsage(Collections.singleton("myindex"), Collections.emptyList(), Collections.emptyList()))
            );
        }

        {
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
                                .settings(indexSettings(Version.CURRENT, 1, 0).put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy"))
                        )
                        .putCustom(
                            ComposableIndexTemplateMetadata.TYPE,
                            new ComposableIndexTemplateMetadata(
                                Collections.singletonMap(
                                    "mytemplate",
                                    new ComposableIndexTemplate(
                                        Collections.singletonList("myds"),
                                        new Template(
                                            Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy").build(),
                                            null,
                                            null
                                        ),
                                        null,
                                        null,
                                        null,
                                        null,
                                        new ComposableIndexTemplate.DataStreamTemplate(false, false)
                                    )
                                )
                            )
                        )
                        .build()
                )
                .build();
            assertThat(
                LifecyclePolicyUtils.calculateUsage(iner, state, "mypolicy"),
                equalTo(new ItemUsage(Collections.singleton("myindex"), Collections.emptyList(), Collections.singleton("mytemplate")))
            );
        }

        {
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
                        .settings(indexSettings(Version.CURRENT, 1, 0).put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy"))
                )
                .put(
                    IndexMetadata.builder("another")
                        .settings(indexSettings(Version.CURRENT, 1, 0).put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy"))
                )
                .put(
                    IndexMetadata.builder("other")
                        .settings(indexSettings(Version.CURRENT, 1, 0).put(LifecycleSettings.LIFECYCLE_NAME, "otherpolicy"))
                )

                .putCustom(
                    ComposableIndexTemplateMetadata.TYPE,
                    new ComposableIndexTemplateMetadata(
                        Collections.singletonMap(
                            "mytemplate",
                            new ComposableIndexTemplate(
                                Collections.singletonList("myds"),
                                new Template(Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy").build(), null, null),
                                null,
                                null,
                                null,
                                null,
                                new ComposableIndexTemplate.DataStreamTemplate(false, false)
                            )
                        )
                    )
                );
            // Need to get the real Index instance of myindex:
            mBuilder.put(DataStreamTestHelper.newInstance("myds", Collections.singletonList(mBuilder.get("myindex").getIndex())));

            // Test where policy exists and is used by an index, datastream, and template
            ClusterState state = ClusterState.builder(new ClusterName("mycluster")).metadata(mBuilder.build()).build();
            assertThat(
                LifecyclePolicyUtils.calculateUsage(iner, state, "mypolicy"),
                equalTo(
                    new ItemUsage(Arrays.asList("myindex", "another"), Collections.singleton("myds"), Collections.singleton("mytemplate"))
                )
            );
        }
    }
}
