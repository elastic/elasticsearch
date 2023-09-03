/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleExplainResponse;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.WaitForRolloverReadyStep;
import org.elasticsearch.xpack.ilm.IndexLifecycleService;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.xpack.ilm.action.TransportExplainLifecycleAction.getIndexLifecycleExplainResponse;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportExplainLifecycleActionTests extends ESTestCase {

    public static final String PHASE_DEFINITION = """
        {
          "policy" : "my-policy",
          "phase_definition" : {
            "min_age" : "20m",
            "actions" : {
              "rollover" : {
                "max_age" : "5s"
              }
            }
          },
          "version" : 1,
          "modified_date_in_millis" : 1578521007076
        }""";

    private static final NamedXContentRegistry REGISTRY;

    static {
        REGISTRY = new NamedXContentRegistry(
            List.of(new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(RolloverAction.NAME), RolloverAction::parse))
        );
    }

    public void testGetIndexLifecycleExplainResponse() throws IOException {
        {
            // only errors index
            IndexLifecycleService indexLifecycleService = mock(IndexLifecycleService.class);
            when(indexLifecycleService.policyExists("my-policy")).thenReturn(true);

            LifecycleExecutionState.Builder errorStepState = LifecycleExecutionState.builder()
                .setPhase("hot")
                .setAction("rollover")
                .setStep(ErrorStep.NAME)
                .setPhaseDefinition(PHASE_DEFINITION);
            String indexInErrorStep = "index_in_error";
            IndexMetadata indexMetadata = IndexMetadata.builder(indexInErrorStep)
                .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, "my-policy"))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5))
                .putCustom(ILM_CUSTOM_METADATA_KEY, errorStepState.build().asMap())
                .build();
            Metadata metadata = Metadata.builder().put(indexMetadata, true).build();

            IndexLifecycleExplainResponse onlyErrorsResponse = getIndexLifecycleExplainResponse(
                indexInErrorStep,
                metadata,
                true,
                true,
                indexLifecycleService,
                REGISTRY
            );
            assertThat(onlyErrorsResponse, notNullValue());
            assertThat(onlyErrorsResponse.getIndex(), is(indexInErrorStep));
            assertThat(onlyErrorsResponse.getStep(), is(ErrorStep.NAME));
        }

        {
            // only managed index
            IndexLifecycleService indexLifecycleService = mock(IndexLifecycleService.class);
            when(indexLifecycleService.policyExists("my-policy")).thenReturn(true);

            LifecycleExecutionState.Builder checkRolloverReadyStepState = LifecycleExecutionState.builder()
                .setPhase("hot")
                .setAction("rollover")
                .setStep(WaitForRolloverReadyStep.NAME)
                .setPhaseDefinition(PHASE_DEFINITION);

            String indexInCheckRolloverStep = "index_in_check_rollover";
            IndexMetadata indexMetadata = IndexMetadata.builder(indexInCheckRolloverStep)
                .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, "my-policy"))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5))
                .putCustom(ILM_CUSTOM_METADATA_KEY, checkRolloverReadyStepState.build().asMap())
                .build();
            Metadata metadata = Metadata.builder().put(indexMetadata, true).build();

            IndexLifecycleExplainResponse onlyErrorsResponse = getIndexLifecycleExplainResponse(
                indexInCheckRolloverStep,
                metadata,
                true,
                true,
                indexLifecycleService,
                REGISTRY
            );
            assertThat(onlyErrorsResponse, nullValue());

            IndexLifecycleExplainResponse allManagedResponse = getIndexLifecycleExplainResponse(
                indexInCheckRolloverStep,
                metadata,
                false,
                true,
                indexLifecycleService,
                REGISTRY
            );
            assertThat(allManagedResponse, notNullValue());
            assertThat(allManagedResponse.getIndex(), is(indexInCheckRolloverStep));
            assertThat(allManagedResponse.getStep(), is(WaitForRolloverReadyStep.NAME));
        }

        {
            // index with missing policy
            IndexLifecycleService indexLifecycleService = mock(IndexLifecycleService.class);
            when(indexLifecycleService.policyExists("random-policy")).thenReturn(false);

            String indexWithMissingPolicy = "index_with_missing_policy";
            IndexMetadata indexMetadata = IndexMetadata.builder(indexWithMissingPolicy)
                .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, "random-policy"))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5))
                .build();
            Metadata metadata = Metadata.builder().put(indexMetadata, true).build();

            IndexLifecycleExplainResponse onlyErrorsResponse = getIndexLifecycleExplainResponse(
                indexWithMissingPolicy,
                metadata,
                true,
                true,
                indexLifecycleService,
                REGISTRY
            );
            assertThat(onlyErrorsResponse, notNullValue());
            assertThat(onlyErrorsResponse.getPolicyName(), is("random-policy"));
        }

        {
            // not managed index
            IndexLifecycleService indexLifecycleService = mock(IndexLifecycleService.class);
            when(indexLifecycleService.policyExists(anyString())).thenReturn(true);

            IndexMetadata indexMetadata = IndexMetadata.builder("index")
                .settings(settings(IndexVersion.current()))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5))
                .build();
            Metadata metadata = Metadata.builder().put(indexMetadata, true).build();

            IndexLifecycleExplainResponse onlyManaged = getIndexLifecycleExplainResponse(
                "index",
                metadata,
                false,
                true,
                indexLifecycleService,
                REGISTRY
            );
            assertThat(onlyManaged, nullValue());
        }
    }
}
