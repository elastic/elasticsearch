/*
 *
 *  * Licensed to Elasticsearch under one or more contributor
 *  * license agreements. See the NOTICE file distributed with
 *  * this work for additional information regarding copyright
 *  * ownership. Elasticsearch licenses this file to you under
 *  * the Apache License, Version 2.0 (the "License"); you may
 *  * not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.test.ESTestCase;

import java.util.Objects;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

public class AliasOrIndexTests extends ESTestCase {

    public void testHiddenAliasValidation() {
        final String hiddenAliasName = "hidden_alias";
        AliasMetaData hiddenAliasMetadata = new AliasMetaData.Builder(hiddenAliasName).isHidden(true).build();

        IndexMetaData hidden1 = buildIndexWithAlias("hidden1", hiddenAliasName, true);
        IndexMetaData hidden2 = buildIndexWithAlias("hidden2", hiddenAliasName, true);
        IndexMetaData hidden3 = buildIndexWithAlias("hidden3", hiddenAliasName, true);

        IndexMetaData indexWithNonHiddenAlias = buildIndexWithAlias("nonhidden1", hiddenAliasName, false);
        IndexMetaData indexWithUnspecifiedAlias = buildIndexWithAlias("nonhidden2", hiddenAliasName, null);

        {
            AliasOrIndex.Alias allHidden = new AliasOrIndex.Alias(hiddenAliasMetadata, hidden1);
            allHidden.addIndex(hidden2);
            allHidden.addIndex(hidden3);
            allHidden.computeAndValidateAliasProperties(); // Should be ok
        }

        {
            AliasOrIndex.Alias allVisible;
            if (randomBoolean()) {
                allVisible = new AliasOrIndex.Alias(hiddenAliasMetadata, indexWithNonHiddenAlias);
                allVisible.addIndex(indexWithUnspecifiedAlias);
            } else {
                allVisible = new AliasOrIndex.Alias(hiddenAliasMetadata, indexWithUnspecifiedAlias);
                allVisible.addIndex(indexWithNonHiddenAlias);
            }

            allVisible.computeAndValidateAliasProperties(); // Should be ok
        }

        {
            AliasOrIndex.Alias oneNonHidden = new AliasOrIndex.Alias(hiddenAliasMetadata, hidden1);
            oneNonHidden.addIndex(hidden2);
            oneNonHidden.addIndex(hidden3);
            oneNonHidden.addIndex(indexWithNonHiddenAlias);
            IllegalStateException exception = expectThrows(IllegalStateException.class,
                () -> oneNonHidden.computeAndValidateAliasProperties());
            assertThat(exception.getMessage(), containsString("alias [" + hiddenAliasName + "] is hidden set as on ["));
            assertThat(exception.getMessage(), allOf(containsString(hidden1.getIndex().getName()),
                containsString(hidden2.getIndex().getName()),
                containsString(hidden3.getIndex().getName())));
            assertThat(exception.getMessage(), containsString("but as NOT hidden on [" +
                indexWithNonHiddenAlias.getIndex().getName() + "]; alias must have same hidden setting on all indices"));
        }

        {
            AliasOrIndex.Alias oneUnspecified = new AliasOrIndex.Alias(hiddenAliasMetadata, hidden1);
            oneUnspecified.addIndex(hidden2);
            oneUnspecified.addIndex(hidden3);
            oneUnspecified.addIndex(indexWithUnspecifiedAlias);
            IllegalStateException exception = expectThrows(IllegalStateException.class,
                () -> oneUnspecified.computeAndValidateAliasProperties());
            assertThat(exception.getMessage(), containsString("alias [" + hiddenAliasName + "] is hidden set as on ["));
            assertThat(exception.getMessage(), allOf(containsString(hidden1.getIndex().getName()),
                containsString(hidden2.getIndex().getName()),
                containsString(hidden3.getIndex().getName())));
            assertThat(exception.getMessage(), containsString("but as NOT hidden on [" +
                indexWithUnspecifiedAlias.getIndex().getName() + "]; alias must have same hidden setting on all indices"));
        }

        {
            AliasOrIndex.Alias mostlyVisibleOneHidden;
            if (randomBoolean()) {
                mostlyVisibleOneHidden = new AliasOrIndex.Alias(hiddenAliasMetadata, indexWithNonHiddenAlias);
                mostlyVisibleOneHidden.addIndex(indexWithUnspecifiedAlias);
            } else {
                mostlyVisibleOneHidden = new AliasOrIndex.Alias(hiddenAliasMetadata, indexWithUnspecifiedAlias);
                mostlyVisibleOneHidden.addIndex(indexWithNonHiddenAlias);
            }
            final IndexMetaData hiddenIndex = randomFrom(hidden1, hidden2, hidden3);
            mostlyVisibleOneHidden.addIndex(hiddenIndex);
            IllegalStateException exception = expectThrows(IllegalStateException.class,
                () -> mostlyVisibleOneHidden.computeAndValidateAliasProperties());
            assertThat(exception.getMessage(), containsString("alias [" + hiddenAliasName + "] is hidden set as on [" +
                hiddenIndex.getIndex().getName() + "] but as NOT hidden on ["));
            assertThat(exception.getMessage(), allOf(containsString(indexWithUnspecifiedAlias.getIndex().getName()),
                containsString(indexWithNonHiddenAlias.getIndex().getName())));
            assertThat(exception.getMessage(), containsString("but as NOT hidden on ["));
        }
    }

    private IndexMetaData buildIndexWithAlias(String indexName, String aliasName, @Nullable Boolean aliasIsHidden) {
        final AliasMetaData.Builder aliasMetaData = new AliasMetaData.Builder(aliasName);
        if (Objects.nonNull(aliasIsHidden) || randomBoolean()) {
            aliasMetaData.isHidden(aliasIsHidden);
        }
        return new IndexMetaData.Builder(indexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(aliasMetaData)
            .build();
    }

}
