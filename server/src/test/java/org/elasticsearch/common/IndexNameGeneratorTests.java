/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.test.ESTestCase;

import java.util.Locale;

import static org.elasticsearch.common.IndexNameGenerator.ILLEGAL_INDEXNAME_CHARS_REGEX;
import static org.elasticsearch.common.IndexNameGenerator.generateValidIndexName;
import static org.elasticsearch.common.IndexNameGenerator.generateValidIndexSuffix;
import static org.elasticsearch.common.IndexNameGenerator.validateGeneratedIndexName;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class IndexNameGeneratorTests extends ESTestCase {

    public void testGenerateValidIndexName() {
        String prefix = randomAlphaOfLengthBetween(5, 15);
        String indexName = randomAlphaOfLengthBetween(5, 100);

        String generatedValidIndexName = generateValidIndexName(prefix, indexName);
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
            IllegalArgumentException illegalArgumentException = expectThrows(
                IllegalArgumentException.class,
                () -> generateValidIndexSuffix(() -> "****???><><>,# \\/:||")
            );
            assertThat(illegalArgumentException.getMessage(), is("unable to generate random index name suffix"));
        }

        {
            assertThat(generateValidIndexSuffix(() -> "LegalChars|||# *"), is("legalchars"));
        }
    }

    public void testValidateGeneratedIndexName() {
        {
            assertThat(
                validateGeneratedIndexName(
                    generateValidIndexName(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 150)),
                    ClusterState.EMPTY_STATE
                ),
                nullValue()
            );
        }

        {
            // index name is validated (invalid chars etc)
            String generatedIndexName = generateValidIndexName("_prefix-", randomAlphaOfLengthBetween(5, 150));
            assertThat(
                validateGeneratedIndexName(generatedIndexName, ClusterState.EMPTY_STATE).validationErrors(),
                containsInAnyOrder("Invalid index name [" + generatedIndexName + "], must not start with '_', '-', or '+'")
            );
        }

        {
            // index name is validated (invalid chars etc)
            String generatedIndexName = generateValidIndexName("shrink-", "shrink-indexName-random###");
            assertThat(
                validateGeneratedIndexName(generatedIndexName, ClusterState.EMPTY_STATE).validationErrors(),
                containsInAnyOrder("Invalid index name [" + generatedIndexName + "], must not contain '#'")
            );
        }

        {
            // generated index already exists as a standalone index
            String generatedIndexName = generateValidIndexName(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 150));
            IndexMetadata indexMetadata = IndexMetadata.builder(generatedIndexName)
                .settings(settings(IndexVersion.current()))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(1, 5))
                .build();
            ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .metadata(Metadata.builder().put(indexMetadata, false))
                .build();

            ActionRequestValidationException validationException = validateGeneratedIndexName(generatedIndexName, clusterState);
            assertThat(validationException, notNullValue());
            assertThat(
                validationException.validationErrors(),
                containsInAnyOrder("the index name we generated [" + generatedIndexName + "] already exists")
            );
        }

        {
            // generated index name already exists as an index (cluster state routing table is also populated)
            String generatedIndexName = generateValidIndexName(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 150));
            IndexMetadata indexMetadata = IndexMetadata.builder(generatedIndexName)
                .settings(settings(IndexVersion.current()))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(1, 5))
                .build();
            ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .routingTable(RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(indexMetadata).build())
                .metadata(Metadata.builder().put(indexMetadata, false))
                .build();

            ActionRequestValidationException validationException = validateGeneratedIndexName(generatedIndexName, clusterState);
            assertThat(validationException, notNullValue());
            assertThat(
                validationException.validationErrors(),
                containsInAnyOrder("the index name we generated [" + generatedIndexName + "] already exists")
            );
            ;
        }

        {
            // generated index name already exists as an alias to another index
            String generatedIndexName = generateValidIndexName(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 150));
            IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLengthBetween(10, 30))
                .settings(settings(IndexVersion.current()))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(1, 5))
                .putAlias(AliasMetadata.builder(generatedIndexName).build())
                .build();
            ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .metadata(Metadata.builder().put(indexMetadata, false))
                .build();

            ActionRequestValidationException validationException = validateGeneratedIndexName(generatedIndexName, clusterState);
            assertThat(validationException, notNullValue());
            assertThat(
                validationException.validationErrors(),
                containsInAnyOrder("the index name we generated [" + generatedIndexName + "] already exists as alias")
            );
        }
    }
}
