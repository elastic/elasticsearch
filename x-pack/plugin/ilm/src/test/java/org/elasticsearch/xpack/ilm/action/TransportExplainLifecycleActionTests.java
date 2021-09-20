/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleExplainResponse;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.WaitForRolloverReadyStep;
import org.elasticsearch.xpack.ilm.IndexLifecycleService;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.xpack.ilm.action.TransportExplainLifecycleAction.getIndexLifecycleExplainResponse;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportExplainLifecycleActionTests extends ESTestCase {

    public static final String PHASE_DEFINITION = "{\n" +
        "        \"policy\" : \"my-policy\",\n" +
        "        \"phase_definition\" : {\n" +
        "          \"min_age\" : \"20m\",\n" +
        "          \"actions\" : {\n" +
        "            \"rollover\" : {\n" +
        "              \"max_age\" : \"5s\"\n" +
        "            }\n" +
        "          }\n" +
        "        },\n" +
        "        \"version\" : 1,\n" +
        "        \"modified_date_in_millis\" : 1578521007076\n" +
        "      }";

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
            IndexMetadata meta = IndexMetadata.builder(indexInErrorStep)
                .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, "my-policy"))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5))
                .putCustom(ILM_CUSTOM_METADATA_KEY, errorStepState.build().asMap())
                .build();

            IndexLifecycleExplainResponse onlyErrorsResponse = getIndexLifecycleExplainResponse(meta, true, true, indexLifecycleService,
                REGISTRY);
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
            IndexMetadata meta = IndexMetadata.builder(indexInCheckRolloverStep)
                .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, "my-policy"))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5))
                .putCustom(ILM_CUSTOM_METADATA_KEY, checkRolloverReadyStepState.build().asMap())
                .build();

            IndexLifecycleExplainResponse onlyErrorsResponse = getIndexLifecycleExplainResponse(meta, true, true, indexLifecycleService,
                REGISTRY);
            assertThat(onlyErrorsResponse, nullValue());

            IndexLifecycleExplainResponse allManagedResponse = getIndexLifecycleExplainResponse(meta, false, true, indexLifecycleService,
                REGISTRY);
            assertThat(allManagedResponse, notNullValue());
            assertThat(allManagedResponse.getIndex(), is(indexInCheckRolloverStep));
            assertThat(allManagedResponse.getStep(), is(WaitForRolloverReadyStep.NAME));
        }

        {
            // index with missing policy
            IndexLifecycleService indexLifecycleService = mock(IndexLifecycleService.class);
            when(indexLifecycleService.policyExists("random-policy")).thenReturn(false);

            String indexWithMissingPolicy = "index_with_missing_policy";
            IndexMetadata meta = IndexMetadata.builder(indexWithMissingPolicy)
                .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, "random-policy"))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5))
                .build();

            IndexLifecycleExplainResponse onlyErrorsResponse = getIndexLifecycleExplainResponse(meta, true, true, indexLifecycleService,
                REGISTRY);
            assertThat(onlyErrorsResponse, notNullValue());
            assertThat(onlyErrorsResponse.getPolicyName(), is("random-policy"));
        }

        {
            // not managed index
            IndexLifecycleService indexLifecycleService = mock(IndexLifecycleService.class);
            when(indexLifecycleService.policyExists(anyString())).thenReturn(true);

            IndexMetadata meta = IndexMetadata.builder("index")
                .settings(settings(Version.CURRENT))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5))
                .build();

            IndexLifecycleExplainResponse onlyManaged = getIndexLifecycleExplainResponse(meta, false, true, indexLifecycleService,
                REGISTRY);
            assertThat(onlyManaged, nullValue());
        }
    }
}
