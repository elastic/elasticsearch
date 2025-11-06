/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleExplainResponse;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadataTests;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.WaitForRolloverReadyStep;
import org.elasticsearch.xpack.ilm.IndexLifecycleService;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.xpack.ilm.action.TransportExplainLifecycleAction.getIndexLifecycleExplainResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportExplainLifecycleActionTests extends ESTestCase {

    private static final String POLICY_NAME = "my-policy";
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

    public void testGetIndexLifecycleExplainResponse_onlyErrors() throws IOException {
        LifecycleExecutionState.Builder errorStepState = LifecycleExecutionState.builder()
            .setPhase("hot")
            .setAction("rollover")
            .setStep(ErrorStep.NAME)
            .setPhaseDefinition(PHASE_DEFINITION);
        String indexInErrorStep = "index_in_error";
        IndexMetadata indexMetadata = IndexMetadata.builder(indexInErrorStep)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, POLICY_NAME))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .putCustom(ILM_CUSTOM_METADATA_KEY, errorStepState.build().asMap())
            .build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(indexMetadata, true)
            .putCustom(IndexLifecycleMetadata.TYPE, createIndexLifecycleMetadata())
            .build();

        IndexLifecycleExplainResponse onlyErrorsResponse = getIndexLifecycleExplainResponse(
            indexInErrorStep,
            project,
            true,
            true,
            REGISTRY,
            randomBoolean()
        );
        assertThat(onlyErrorsResponse, notNullValue());
        assertThat(onlyErrorsResponse.getIndex(), is(indexInErrorStep));
        assertThat(onlyErrorsResponse.getStep(), is(ErrorStep.NAME));
    }

    public void testGetIndexLifecycleExplainResponse_onlyManagedIndex() throws IOException {
        LifecycleExecutionState.Builder checkRolloverReadyStepState = LifecycleExecutionState.builder()
            .setPhase("hot")
            .setAction("rollover")
            .setStep(WaitForRolloverReadyStep.NAME)
            .setPhaseDefinition(PHASE_DEFINITION);

        String indexInCheckRolloverStep = "index_in_check_rollover";
        IndexMetadata indexMetadata = IndexMetadata.builder(indexInCheckRolloverStep)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, POLICY_NAME))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .putCustom(ILM_CUSTOM_METADATA_KEY, checkRolloverReadyStepState.build().asMap())
            .build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(indexMetadata, true)
            .putCustom(IndexLifecycleMetadata.TYPE, createIndexLifecycleMetadata())
            .build();

        IndexLifecycleExplainResponse onlyErrorsResponse = getIndexLifecycleExplainResponse(
            indexInCheckRolloverStep,
            project,
            true,
            true,
            REGISTRY,
            randomBoolean()
        );
        assertThat(onlyErrorsResponse, nullValue());

        IndexLifecycleExplainResponse allManagedResponse = getIndexLifecycleExplainResponse(
            indexInCheckRolloverStep,
            project,
            false,
            true,
            REGISTRY,
            randomBoolean()
        );
        assertThat(allManagedResponse, notNullValue());
        assertThat(allManagedResponse.getIndex(), is(indexInCheckRolloverStep));
        assertThat(allManagedResponse.getStep(), is(WaitForRolloverReadyStep.NAME));
    }

    public void testGetIndexLifecycleExplainResponse_indexWithMissingPolicy() throws IOException {
        IndexLifecycleService indexLifecycleService = mock(IndexLifecycleService.class);
        when(indexLifecycleService.policyExists("random-policy")).thenReturn(false);

        String indexWithMissingPolicy = "index_with_missing_policy";
        IndexMetadata indexMetadata = IndexMetadata.builder(indexWithMissingPolicy)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, "random-policy"))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(indexMetadata, true)
            .putCustom(IndexLifecycleMetadata.TYPE, createIndexLifecycleMetadata())
            .build();

        IndexLifecycleExplainResponse onlyErrorsResponse = getIndexLifecycleExplainResponse(
            indexWithMissingPolicy,
            project,
            true,
            true,
            REGISTRY,
            randomBoolean()
        );
        assertThat(onlyErrorsResponse, notNullValue());
        assertThat(onlyErrorsResponse.getPolicyName(), is("random-policy"));
        assertThat(onlyErrorsResponse.getStep(), is(ErrorStep.NAME));
    }

    public void testGetIndexLifecycleExplainResponse_notManagedIndex() throws IOException {
        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(indexMetadata, true)
            .putCustom(IndexLifecycleMetadata.TYPE, createIndexLifecycleMetadata())
            .build();

        IndexLifecycleExplainResponse onlyManaged = getIndexLifecycleExplainResponse(
            "index",
            project,
            false,
            true,
            REGISTRY,
            randomBoolean()
        );
        assertThat(onlyManaged, nullValue());
    }

    public void testGetIndexLifecycleExplainResponse_rolloverOnlyIfHasDocuments_addsCondition() throws IOException {
        LifecycleExecutionState.Builder checkRolloverReadyStepState = LifecycleExecutionState.builder()
            .setPhase("hot")
            .setAction("rollover")
            .setStep(WaitForRolloverReadyStep.NAME)
            .setPhaseDefinition(PHASE_DEFINITION);
        String indexInCheckRolloverStep = "index_in_check_rollover";
        IndexMetadata indexMetadata = IndexMetadata.builder(indexInCheckRolloverStep)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, POLICY_NAME))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .putCustom(ILM_CUSTOM_METADATA_KEY, checkRolloverReadyStepState.build().asMap())
            .build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(indexMetadata, true)
            .putCustom(IndexLifecycleMetadata.TYPE, createIndexLifecycleMetadata())
            .build();

        IndexLifecycleExplainResponse response = getIndexLifecycleExplainResponse(
            indexInCheckRolloverStep,
            project,
            false,
            true,
            REGISTRY,
            true
        );
        var rolloverAction = ((RolloverAction) response.getPhaseExecutionInfo().getPhase().getActions().get(RolloverAction.NAME));
        assertThat(rolloverAction, notNullValue());
        assertThat(rolloverAction.getConditions().getMinDocs(), is(1L));
    }

    public void testPreviousStepInfoTruncationDoesNotBreakExplainJson() throws Exception {
        final String policyName = "policy";
        final String indexName = "index";

        final String longReasonMessage = "a".repeat(LifecycleExecutionState.MAXIMUM_STEP_INFO_STRING_LENGTH);
        final String errorJsonWhichWillBeTruncated = Strings.toString((builder, params) -> {
            ElasticsearchException.generateThrowableXContent(
                builder,
                ToXContent.EMPTY_PARAMS,
                new IllegalArgumentException(longReasonMessage)
            );
            return builder;
        });

        final LifecycleExecutionState stateWithTruncatedStepInfo = LifecycleExecutionState.builder()
            .setPhase("hot")
            .setAction("rollover")
            .setStep("some_step")
            .setPhaseDefinition(PHASE_DEFINITION)
            .setStepInfo(errorJsonWhichWillBeTruncated)
            .build();

        // Simulate transition to next step where previous_step_info is copied
        final LifecycleExecutionState stateWithPreviousStepInfo = LifecycleExecutionState.builder(stateWithTruncatedStepInfo)
            .setPreviousStepInfo(stateWithTruncatedStepInfo.stepInfo())
            .setStepInfo(null)
            .build();

        final IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putCustom(ILM_CUSTOM_METADATA_KEY, stateWithPreviousStepInfo.asMap())
            .build();

        final ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(indexMetadata, true)
            .putCustom(
                IndexLifecycleMetadata.TYPE,
                new IndexLifecycleMetadata(
                    Map.of(policyName, LifecyclePolicyMetadataTests.createRandomPolicyMetadata(policyName)),
                    randomFrom(OperationMode.values())
                )
            )
            .build();

        final IndexLifecycleExplainResponse response = TransportExplainLifecycleAction.getIndexLifecycleExplainResponse(
            indexName,
            project,
            false,
            true,
            REGISTRY,
            randomBoolean()
        );

        final String serialized = Strings.toString(response);
        // test we produce valid JSON
        try (
            XContentParser p = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY.withRegistry(REGISTRY), serialized)
        ) {
            final IndexLifecycleExplainResponse deserialized = IndexLifecycleExplainResponse.PARSER.apply(p, null);
            assertThat(deserialized.toString(), equalTo(response.toString()));
            final String actualPreviousStepInfo = deserialized.getPreviousStepInfo().utf8ToString();

            final String expectedPreviousStepInfoFormat = """
                {"type":"illegal_argument_exception","reason":"%s"}""";
            final int jsonBaseLength = Strings.format(expectedPreviousStepInfoFormat, "").length();
            final String expectedReason = Strings.format(
                "%s... (%d chars truncated)",
                "a".repeat(LifecycleExecutionState.MAXIMUM_STEP_INFO_STRING_LENGTH - jsonBaseLength),
                jsonBaseLength
            );
            final String expectedPreviousStepInfo = Strings.format(expectedPreviousStepInfoFormat, expectedReason);
            assertEquals(actualPreviousStepInfo, expectedPreviousStepInfo);
        }
    }

    private static IndexLifecycleMetadata createIndexLifecycleMetadata() {
        return new IndexLifecycleMetadata(
            Map.of(POLICY_NAME, LifecyclePolicyMetadataTests.createRandomPolicyMetadata(POLICY_NAME)),
            randomFrom(OperationMode.values())
        );
    }
}
