/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesClusterStateUpdateRequest;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class TransportRolloverActionTests extends ESTestCase {

    public void testSatisfyConditions() throws Exception {
        List<Condition> conditions = Collections.emptyList();
        assertTrue(TransportRolloverAction.satisfiesConditions(conditions, randomLong(), randomLong(),
            randomLong()));

        conditions = Collections.singletonList(
            new Condition(Condition.ConditionType.MAX_AGE, 10L));
        assertTrue(TransportRolloverAction.satisfiesConditions(conditions, randomLong(), randomLong(),
            System.currentTimeMillis() - randomIntBetween(10, 100)));
        assertFalse(TransportRolloverAction.satisfiesConditions(conditions, randomLong(), randomLong(),
            System.currentTimeMillis() - randomIntBetween(1, 9)));

        conditions = Collections.singletonList(
            new Condition(Condition.ConditionType.MAX_SIZE, 10L));
        assertTrue(TransportRolloverAction.satisfiesConditions(conditions, randomLong(), randomIntBetween(10, 100),
            randomLong()));
        assertFalse(TransportRolloverAction.satisfiesConditions(conditions, randomLong(), randomIntBetween(1, 9),
            randomLong()));

        conditions = Collections.singletonList(
            new Condition(Condition.ConditionType.MAX_DOCS, 10L));
        assertTrue(TransportRolloverAction.satisfiesConditions(conditions, randomIntBetween(10, 100), randomLong(),
            randomLong()));
        assertFalse(TransportRolloverAction.satisfiesConditions(conditions, randomIntBetween(1, 9), randomLong(),
            randomLong()));

        conditions = Arrays.asList(new Condition(Condition.ConditionType.MAX_AGE, 100L),
            new Condition(Condition.ConditionType.MAX_DOCS, 1000L));
        assertTrue(TransportRolloverAction.satisfiesConditions(conditions, randomIntBetween(1000, 1500),
            randomLong(), System.currentTimeMillis() - randomIntBetween(100, 500)));
        assertFalse(TransportRolloverAction.satisfiesConditions(conditions, randomIntBetween(1, 999),
            randomLong(), System.currentTimeMillis() - randomIntBetween(100, 500)));
        assertFalse(TransportRolloverAction.satisfiesConditions(conditions, randomIntBetween(1000, 1500),
            randomLong(), System.currentTimeMillis() - randomIntBetween(1, 99)));
    }

    public void testCreateUpdateAliasRequest() throws Exception {
        String sourceAlias = randomAsciiOfLength(10);
        String sourceIndex = randomAsciiOfLength(10);
        String targetIndex = randomAsciiOfLength(10);
        final RolloverRequest rolloverRequest = new RolloverRequest(sourceAlias, null);
        final IndicesAliasesClusterStateUpdateRequest updateRequest =
            TransportRolloverAction.prepareIndicesAliasesRequest(sourceIndex, targetIndex, rolloverRequest);

        final AliasAction[] actions = updateRequest.actions();
        assertThat(actions.length, equalTo(2));
        boolean foundAdd = false;
        boolean foundRemove = false;
        for (AliasAction action : actions) {
            if (action.actionType() == AliasAction.Type.ADD) {
                foundAdd = true;
                assertThat(action.index(), equalTo(targetIndex));
                assertThat(action.alias(), equalTo(sourceAlias));
            } else if (action.actionType() == AliasAction.Type.REMOVE) {
                foundRemove = true;
                assertThat(action.index(), equalTo(sourceIndex));
                assertThat(action.alias(), equalTo(sourceAlias));
            }
        }
        assertTrue(foundAdd);
        assertTrue(foundRemove);
    }

    public void testCreateUpdateAliasRequestWithOptionalTargetAlias() throws Exception {
        String sourceAlias = randomAsciiOfLength(10);
        String optionalTargetAlias = randomAsciiOfLength(10);
        String sourceIndex = randomAsciiOfLength(10);
        String targetIndex = randomAsciiOfLength(10);
        final RolloverRequest rolloverRequest = new RolloverRequest(sourceAlias, optionalTargetAlias);
        final IndicesAliasesClusterStateUpdateRequest updateRequest =
            TransportRolloverAction.prepareIndicesAliasesRequest(sourceIndex, targetIndex, rolloverRequest);

        final AliasAction[] actions = updateRequest.actions();
        assertThat(actions.length, equalTo(3));
        boolean foundAdd = false;
        boolean foundRemove = false;
        for (AliasAction action : actions) {
            if (action.actionType() == AliasAction.Type.ADD) {
                foundAdd = true;
                assertThat(action.index(), equalTo(targetIndex));
                assertThat(action.alias(), anyOf(equalTo(sourceAlias),
                    equalTo(optionalTargetAlias)));
            } else if (action.actionType() == AliasAction.Type.REMOVE) {
                foundRemove = true;
                assertThat(action.index(), equalTo(sourceIndex));
                assertThat(action.alias(), equalTo(sourceAlias));
            }
        }
        assertTrue(foundAdd);
        assertTrue(foundRemove);
    }

    public void testValidation() throws Exception {
        String index1 = randomAsciiOfLength(10);
        String alias = randomAsciiOfLength(10);
        String index2 = randomAsciiOfLength(10);
        String aliasWithMultipleIndices = randomAsciiOfLength(10);
        final Settings settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        final MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder(index1)
                .settings(settings)
                .putAlias(AliasMetaData.builder(alias))
                .putAlias(AliasMetaData.builder(aliasWithMultipleIndices))
            )
            .put(IndexMetaData.builder(index2)
                .settings(settings)
                .putAlias(AliasMetaData.builder(aliasWithMultipleIndices))
            ).build();

        try {
            TransportRolloverAction.validate(metaData, new RolloverRequest(aliasWithMultipleIndices,
                randomBoolean() ? null : randomAsciiOfLength(10)));
            fail("expected to throw exception");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("source alias maps to multiple indices"));
        }

        try {
            TransportRolloverAction.validate(metaData, new RolloverRequest(randomFrom(index1, index2),
                randomBoolean() ? null : randomAsciiOfLength(10)));
            fail("expected to throw exception");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("source alias is a concrete index"));
        }

        try {
            TransportRolloverAction.validate(metaData, new RolloverRequest(randomAsciiOfLength(5),
                randomBoolean() ? null : randomAsciiOfLength(10)));
            fail("expected to throw exception");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("source alias does not exist"));
        }

        TransportRolloverAction.validate(metaData, new RolloverRequest(alias,
            randomBoolean() ? null : randomAsciiOfLength(10)));
    }

    public void testGenerateRolloverIndexName() throws Exception {
        String index = randomAsciiOfLength(10);
        assertThat(TransportRolloverAction.generateRolloverIndexName(index), not(equalTo(index)));
        assertThat(TransportRolloverAction.generateRolloverIndexName("index"), equalTo("index-1"));
        assertThat(TransportRolloverAction.generateRolloverIndexName("index-1"), equalTo("index-2"));
        assertThat(TransportRolloverAction.generateRolloverIndexName("index-name-1"), equalTo("index-name-2"));
        assertThat(TransportRolloverAction.generateRolloverIndexName("index-name"), equalTo("index-name-1"));
    }
}
