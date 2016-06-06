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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class TransportRolloverActionTests extends ESTestCase {

    public void testCreateUpdateAliasRequest() throws Exception {
        String sourceAlias = randomAsciiOfLength(10);
        String sourceIndex = randomAsciiOfLength(10);
        String targetIndex = randomAsciiOfLength(10);
        final RolloverRequest rolloverRequest = new RolloverRequest(sourceAlias);
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

        expectThrows(IllegalArgumentException.class, () ->
            TransportRolloverAction.validate(metaData, new RolloverRequest(aliasWithMultipleIndices)));
        expectThrows(IllegalArgumentException.class, () ->
            TransportRolloverAction.validate(metaData, new RolloverRequest(randomFrom(index1, index2))));
        expectThrows(IllegalArgumentException.class, () ->
            TransportRolloverAction.validate(metaData, new RolloverRequest(randomAsciiOfLength(5)))
        );
        TransportRolloverAction.validate(metaData, new RolloverRequest(alias));
    }

    public void testGenerateRolloverIndexName() throws Exception {
        String indexNotEndingInNumbers = randomAsciiOfLength(10) + "A";
        assertThat(TransportRolloverAction.generateRolloverIndexName(indexNotEndingInNumbers),
            not(equalTo(indexNotEndingInNumbers)));
        assertThat(TransportRolloverAction.generateRolloverIndexName(indexNotEndingInNumbers),
            equalTo(indexNotEndingInNumbers + "-1"));
        int num = randomIntBetween(0, 100);
        final String indexPrefix = randomAsciiOfLength(10);
        String indexEndingInNumbers = indexPrefix + "-" + num;
        assertThat(TransportRolloverAction.generateRolloverIndexName(indexEndingInNumbers),
            equalTo(indexPrefix + "-" + (num + 1)));
        assertThat(TransportRolloverAction.generateRolloverIndexName("index-name-1"), equalTo("index-name-2"));
        assertThat(TransportRolloverAction.generateRolloverIndexName("index-name"), equalTo("index-name-1"));
    }
}
