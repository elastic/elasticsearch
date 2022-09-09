/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class SwapAliasesAndDeleteSourceIndexStepTests extends AbstractStepTestCase<SwapAliasesAndDeleteSourceIndexStep> {

    @Override
    public SwapAliasesAndDeleteSourceIndexStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        String restoredIndexPrefix = randomAlphaOfLength(10);
        return new SwapAliasesAndDeleteSourceIndexStep(stepKey, nextStepKey, client, restoredIndexPrefix);
    }

    @Override
    protected SwapAliasesAndDeleteSourceIndexStep copyInstance(SwapAliasesAndDeleteSourceIndexStep instance) {
        return new SwapAliasesAndDeleteSourceIndexStep(
            instance.getKey(),
            instance.getNextStepKey(),
            instance.getClient(),
            instance.getTargetIndexNameSupplier(),
            instance.getCreateSourceIndexAlias()
        );
    }

    @Override
    public SwapAliasesAndDeleteSourceIndexStep mutateInstance(SwapAliasesAndDeleteSourceIndexStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        BiFunction<String, LifecycleExecutionState, String> indexNameSupplier = instance.getTargetIndexNameSupplier();
        boolean createSourceIndexAlias = instance.getCreateSourceIndexAlias();
        switch (between(0, 3)) {
            case 0 -> key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            case 1 -> nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            case 2 -> indexNameSupplier = (index, state) -> index + randomAlphaOfLength(5);
            case 3 -> createSourceIndexAlias = createSourceIndexAlias == false;
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new SwapAliasesAndDeleteSourceIndexStep(key, nextKey, instance.getClient(), indexNameSupplier, createSourceIndexAlias);
    }

    public void testPerformAction() {
        String sourceIndexName = randomAlphaOfLength(10);
        IndexMetadata.Builder sourceIndexMetadataBuilder = IndexMetadata.builder(sourceIndexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5));
        Boolean isHidden = randomFrom(Boolean.TRUE, Boolean.FALSE, null);
        AliasMetadata.Builder aliasBuilder = AliasMetadata.builder(randomAlphaOfLengthBetween(3, 10));
        if (randomBoolean()) {
            aliasBuilder.routing(randomAlphaOfLengthBetween(1, 10));
        }
        if (randomBoolean()) {
            aliasBuilder.searchRouting(randomAlphaOfLengthBetween(1, 10));
        }
        if (randomBoolean()) {
            aliasBuilder.indexRouting(randomAlphaOfLengthBetween(1, 10));
        }
        aliasBuilder.writeIndex(randomBoolean());
        aliasBuilder.isHidden(isHidden);
        AliasMetadata aliasMetadata = aliasBuilder.build();
        IndexMetadata sourceIndexMetadata = sourceIndexMetadataBuilder.putAlias(aliasMetadata).build();

        String targetIndexPrefix = "index_prefix";
        String targetIndexName = targetIndexPrefix + sourceIndexName;

        List<AliasActions> expectedAliasActions = Arrays.asList(
            AliasActions.removeIndex().index(sourceIndexName),
            AliasActions.add().index(targetIndexName).alias(sourceIndexName),
            AliasActions.add()
                .index(targetIndexName)
                .alias(aliasMetadata.alias())
                .searchRouting(aliasMetadata.searchRouting())
                .indexRouting(aliasMetadata.indexRouting())
                .writeIndex(null)
                .isHidden(isHidden)
        );

        try (NoOpClient client = getIndicesAliasAssertingClient(expectedAliasActions)) {
            SwapAliasesAndDeleteSourceIndexStep step = new SwapAliasesAndDeleteSourceIndexStep(
                randomStepKey(),
                randomStepKey(),
                client,
                targetIndexPrefix
            );

            IndexMetadata.Builder targetIndexMetadataBuilder = IndexMetadata.builder(targetIndexName)
                .settings(settings(Version.CURRENT))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5));

            ClusterState clusterState = ClusterState.builder(emptyClusterState())
                .metadata(Metadata.builder().put(sourceIndexMetadata, true).put(targetIndexMetadataBuilder).build())
                .build();

            step.performAction(sourceIndexMetadata, clusterState, null, ActionListener.noop());
        }
    }

    private NoOpClient getIndicesAliasAssertingClient(List<AliasActions> expectedAliasActions) {
        return new NoOpClient(getTestName()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                assertThat(action.name(), is(IndicesAliasesAction.NAME));
                assertTrue(request instanceof IndicesAliasesRequest);
                assertThat(((IndicesAliasesRequest) request).getAliasActions(), equalTo(expectedAliasActions));
            }
        };
    }
}
