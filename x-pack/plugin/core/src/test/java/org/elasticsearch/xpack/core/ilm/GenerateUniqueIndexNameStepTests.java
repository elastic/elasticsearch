/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.Builder;

import java.util.Locale;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.core.ilm.GenerateUniqueIndexNameStep.ILLEGAL_INDEXNAME_CHARS_REGEX;
import static org.elasticsearch.xpack.core.ilm.GenerateUniqueIndexNameStep.generateValidIndexName;
import static org.elasticsearch.xpack.core.ilm.GenerateUniqueIndexNameStep.generateValidIndexSuffix;
import static org.elasticsearch.xpack.core.ilm.GenerateUniqueIndexNameStep.validateGeneratedIndexName;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class GenerateUniqueIndexNameStepTests extends AbstractStepTestCase<GenerateUniqueIndexNameStep> {

    @Override
    protected GenerateUniqueIndexNameStep createRandomInstance() {
        return new GenerateUniqueIndexNameStep(randomStepKey(), randomStepKey(), randomAlphaOfLengthBetween(5, 10), lifecycleStateSetter());
    }

    private static BiFunction<String, Builder, Builder> lifecycleStateSetter() {
        return (generatedIndexName, lifecycleStateBuilder) -> lifecycleStateBuilder.setShrinkIndexName(generatedIndexName);
    }

    @Override
    protected GenerateUniqueIndexNameStep mutateInstance(GenerateUniqueIndexNameStep instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();
        String prefix = instance.prefix();

        switch (between(0, 2)) {
            case 0:
                key = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 1:
                nextKey = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 2:
                prefix = randomValueOtherThan(prefix, () -> randomAlphaOfLengthBetween(5, 10));
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new GenerateUniqueIndexNameStep(key, nextKey, prefix, lifecycleStateSetter());
    }

    @Override
    protected GenerateUniqueIndexNameStep copyInstance(GenerateUniqueIndexNameStep instance) {
        return new GenerateUniqueIndexNameStep(instance.getKey(), instance.getNextStepKey(), instance.prefix(),
            instance.lifecycleStateSetter());
    }

    public void testPerformAction() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";
        IndexMetadata.Builder indexMetadataBuilder =
            IndexMetadata.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5));

        final IndexMetadata indexMetadata = indexMetadataBuilder.build();
        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(Metadata.builder().put(indexMetadata, false).build()).build();

        GenerateUniqueIndexNameStep generateUniqueIndexNameStep = createRandomInstance();
        ClusterState newClusterState = generateUniqueIndexNameStep.performAction(indexMetadata.getIndex(), clusterState);

        LifecycleExecutionState executionState = LifecycleExecutionState.fromIndexMetadata(newClusterState.metadata().index(indexName));
        assertThat("the " + GenerateUniqueIndexNameStep.NAME + " step must generate an index name", executionState.getShrinkIndexName(),
            notNullValue());
        assertThat(executionState.getShrinkIndexName(), containsString(indexName));
        assertThat(executionState.getShrinkIndexName(), startsWith(generateUniqueIndexNameStep.prefix()));
    }

    public void testGenerateValidIndexName() {
        String prefix = randomAlphaOfLengthBetween(5, 15);
        String indexName = randomAlphaOfLengthBetween(5, 100);

        String generatedValidIndexName = GenerateUniqueIndexNameStep.generateValidIndexName(prefix, indexName);
        assertThat(generatedValidIndexName, startsWith(prefix));
        assertThat(generatedValidIndexName, containsString(indexName));
        try {
            MetadataCreateIndexService.validateIndexOrAliasName(generatedValidIndexName, InvalidIndexNameException::new);
        } catch (InvalidIndexNameException e) {
            fail("generated index name [" + generatedValidIndexName + "] which is invalid due to [" + e.getDetailedMessage() + "]");
        }
    }

    public void testGenerateValidIndexSuffix() {
        {
            String indexSuffix = generateValidIndexSuffix(() -> UUIDs.randomBase64UUID().toLowerCase(Locale.ROOT));
            assertThat(indexSuffix, notNullValue());
            assertThat(indexSuffix.length(), greaterThanOrEqualTo(1));
            assertThat(indexSuffix.matches(ILLEGAL_INDEXNAME_CHARS_REGEX), is(false));
        }

        {
            IllegalArgumentException illegalArgumentException = expectThrows(IllegalArgumentException.class,
                () -> generateValidIndexSuffix(() -> "****???><><>,# \\/:||"));
            assertThat(illegalArgumentException.getMessage(), is("unable to generate random index name suffix"));
        }

        {
            assertThat(generateValidIndexSuffix(() -> "LegalChars|||# *"), is("legalchars"));
        }
    }

    public void testValidateGeneratedIndexName() {
        {
            assertThat(validateGeneratedIndexName(
                generateValidIndexName(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 150)), ClusterState.EMPTY_STATE
            ), nullValue());
        }

        {
            // index name is validated (invalid chars etc)
            String generatedIndexName = generateValidIndexName("_prefix-", randomAlphaOfLengthBetween(5, 150));
            assertThat(validateGeneratedIndexName(generatedIndexName, ClusterState.EMPTY_STATE).validationErrors(), containsInAnyOrder(
                "Invalid index name [" + generatedIndexName + "], must not start with '_', '-', or '+'"));
        }

        {
            // index name is validated (invalid chars etc)
            String generatedIndexName = generateValidIndexName("shrink-", "shrink-indexName-random###");
            assertThat(validateGeneratedIndexName(generatedIndexName, ClusterState.EMPTY_STATE).validationErrors(), containsInAnyOrder(
                "Invalid index name [" + generatedIndexName + "], must not contain '#'"));
        }

        {
            // generated index already exists as a standalone index
            String generatedIndexName = generateValidIndexName(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 150));
            IndexMetadata indexMetadata = IndexMetadata.builder(generatedIndexName)
                .settings(settings(Version.CURRENT)).numberOfShards(randomIntBetween(1,5))
                .numberOfReplicas(randomIntBetween(1,5))
                .build();
            ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .metadata(Metadata.builder()
                    .put(indexMetadata, false))
                .build();

            ActionRequestValidationException validationException = validateGeneratedIndexName(generatedIndexName, clusterState);
            assertThat(validationException, notNullValue());
            assertThat(validationException.validationErrors(), containsInAnyOrder("the index name we generated [" + generatedIndexName
                + "] already exists"));
        }

        {
            // generated index name already exists as an index (cluster state routing table is also populated)
            String generatedIndexName = generateValidIndexName(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 150));
            IndexMetadata indexMetadata = IndexMetadata.builder(generatedIndexName)
                .settings(settings(Version.CURRENT)).numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(1, 5))
                .build();
            ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .routingTable(RoutingTable.builder().addAsNew(indexMetadata).build())
                .metadata(Metadata.builder().put(indexMetadata, false))
                .build();

            ActionRequestValidationException validationException = validateGeneratedIndexName(generatedIndexName, clusterState);
            assertThat(validationException, notNullValue());
            assertThat(validationException.validationErrors(), containsInAnyOrder("the index name we generated [" + generatedIndexName
                + "] already exists"));;
        }

        {
            // generated index name already exists as an alias to another index
            String generatedIndexName = generateValidIndexName(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 150));
            IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLengthBetween(10, 30))
                .settings(settings(Version.CURRENT)).numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(1, 5))
                .putAlias(AliasMetadata.builder(generatedIndexName).build())
                .build();
            ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .metadata(Metadata.builder()
                    .put(indexMetadata, false))
                .build();

            ActionRequestValidationException validationException = validateGeneratedIndexName(generatedIndexName, clusterState);
            assertThat(validationException, notNullValue());
            assertThat(validationException.validationErrors(), containsInAnyOrder("the index name we generated [" + generatedIndexName
                + "] already exists as alias"));
        }
    }

    public void testParseOriginationDateFromGeneratedIndexName() {
        String indexName = "testIndex-2021.03.13-000001";
        String generateValidIndexName = generateValidIndexName("shrink-", indexName);
        long extractedDateMillis = IndexLifecycleOriginationDateParser.parseIndexNameAndExtractDate(generateValidIndexName);
        assertThat(extractedDateMillis, is(DateFormatter.forPattern("uuuu.MM.dd").parseMillis("2021.03.13")));
    }
}
