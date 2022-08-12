/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.core.ilm.ShrinkIndexNameSupplier.SHRUNKEN_INDEX_PREFIX;
import static org.hamcrest.Matchers.equalTo;

public class ShrinkSetAliasStepTests extends AbstractStepTestCase<ShrinkSetAliasStep> {

    @Override
    public ShrinkSetAliasStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        return new ShrinkSetAliasStep(stepKey, nextStepKey, client);
    }

    @Override
    public ShrinkSetAliasStep mutateInstance(ShrinkSetAliasStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        switch (between(0, 1)) {
            case 0 -> key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            case 1 -> nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new ShrinkSetAliasStep(key, nextKey, instance.getClient());
    }

    @Override
    public ShrinkSetAliasStep copyInstance(ShrinkSetAliasStep instance) {
        return new ShrinkSetAliasStep(instance.getKey(), instance.getNextStepKey(), instance.getClient());
    }

    public void testPerformAction() throws Exception {
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(randomAlphaOfLength(10))
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5));
        AliasMetadata.Builder aliasBuilder = AliasMetadata.builder(randomAlphaOfLengthBetween(3, 10));
        if (randomBoolean()) {
            aliasBuilder.routing(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            aliasBuilder.searchRouting(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            aliasBuilder.indexRouting(randomAlphaOfLengthBetween(3, 10));
        }
        String aliasMetadataFilter = randomBoolean() ? null : "{\"term\":{\"year\":2016}}";
        aliasBuilder.filter(aliasMetadataFilter);
        aliasBuilder.writeIndex(randomBoolean());
        AliasMetadata aliasMetadata = aliasBuilder.build();
        IndexMetadata indexMetadata = indexMetadataBuilder.putAlias(aliasMetadata).build();
        ShrinkSetAliasStep step = createRandomInstance();

        String sourceIndex = indexMetadata.getIndex().getName();
        String shrunkenIndex = SHRUNKEN_INDEX_PREFIX + sourceIndex;
        List<AliasActions> expectedAliasActions = Arrays.asList(
            IndicesAliasesRequest.AliasActions.removeIndex().index(sourceIndex),
            IndicesAliasesRequest.AliasActions.add().index(shrunkenIndex).alias(sourceIndex),
            IndicesAliasesRequest.AliasActions.add()
                .index(shrunkenIndex)
                .alias(aliasMetadata.alias())
                .searchRouting(aliasMetadata.searchRouting())
                .indexRouting(aliasMetadata.indexRouting())
                .filter(aliasMetadataFilter)
                .writeIndex(null)
        );

        Mockito.doAnswer(invocation -> {
            IndicesAliasesRequest request = (IndicesAliasesRequest) invocation.getArguments()[0];
            assertThat(request.getAliasActions(), equalTo(expectedAliasActions));
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[1];
            listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(indicesClient).aliases(Mockito.any(), Mockito.any());

        PlainActionFuture.<Void, Exception>get(f -> step.performAction(indexMetadata, emptyClusterState(), null, f));

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).aliases(Mockito.any(), Mockito.any());
    }

    public void testPerformActionFailure() {
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        Exception exception = new RuntimeException();
        ShrinkSetAliasStep step = createRandomInstance();

        Mockito.doAnswer((Answer<Void>) invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[1];
            listener.onFailure(exception);
            return null;
        }).when(indicesClient).aliases(Mockito.any(), Mockito.any());

        assertSame(
            exception,
            expectThrows(
                Exception.class,
                () -> PlainActionFuture.<Void, Exception>get(f -> step.performAction(indexMetadata, emptyClusterState(), null, f))
            )
        );

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).aliases(Mockito.any(), Mockito.any());
    }

}
