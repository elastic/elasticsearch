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
import org.elasticsearch.test.VersionUtils;

import java.util.Objects;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

public class IndexAbstractionTests extends ESTestCase {

    public static final String SYSTEM_ALIAS_NAME = "system_alias";

    public void testHiddenAliasValidation() {
        final String hiddenAliasName = "hidden_alias";
        AliasMetadata hiddenAliasMetadata = new AliasMetadata.Builder(hiddenAliasName).isHidden(true).build();

        IndexMetadata hidden1 = buildIndexWithAlias("hidden1", hiddenAliasName, true, Version.CURRENT, false);
        IndexMetadata hidden2 = buildIndexWithAlias("hidden2", hiddenAliasName, true, Version.CURRENT, false);
        IndexMetadata hidden3 = buildIndexWithAlias("hidden3", hiddenAliasName, true, Version.CURRENT, false);

        IndexMetadata indexWithNonHiddenAlias = buildIndexWithAlias("nonhidden1", hiddenAliasName, false, Version.CURRENT, false);
        IndexMetadata indexWithUnspecifiedAlias = buildIndexWithAlias("nonhidden2", hiddenAliasName, null, Version.CURRENT, false);

        {
            IndexAbstraction.Alias allHidden = new IndexAbstraction.Alias(hiddenAliasMetadata, hidden1);
            allHidden.addIndex(hidden2);
            allHidden.addIndex(hidden3);
            allHidden.computeAndValidateAliasProperties(); // Should be ok
        }

        {
            IndexAbstraction.Alias allVisible;
            if (randomBoolean()) {
                allVisible = new IndexAbstraction.Alias(hiddenAliasMetadata, indexWithNonHiddenAlias);
                allVisible.addIndex(indexWithUnspecifiedAlias);
            } else {
                allVisible = new IndexAbstraction.Alias(hiddenAliasMetadata, indexWithUnspecifiedAlias);
                allVisible.addIndex(indexWithNonHiddenAlias);
            }

            allVisible.computeAndValidateAliasProperties(); // Should be ok
        }

        {
            IndexAbstraction.Alias oneNonHidden = new IndexAbstraction.Alias(hiddenAliasMetadata, hidden1);
            oneNonHidden.addIndex(hidden2);
            oneNonHidden.addIndex(hidden3);
            oneNonHidden.addIndex(indexWithNonHiddenAlias);
            IllegalStateException exception = expectThrows(IllegalStateException.class,
                () -> oneNonHidden.computeAndValidateAliasProperties());
            assertThat(exception.getMessage(), containsString("alias [" + hiddenAliasName +
                "] has is_hidden set to true on indices ["));
            assertThat(exception.getMessage(), allOf(containsString(hidden1.getIndex().getName()),
                containsString(hidden2.getIndex().getName()),
                containsString(hidden3.getIndex().getName())));
            assertThat(exception.getMessage(), containsString("but does not have is_hidden set to true on indices [" +
                indexWithNonHiddenAlias.getIndex().getName() + "]; alias must have the same is_hidden setting on all indices"));
        }

        {
            IndexAbstraction.Alias oneUnspecified = new IndexAbstraction.Alias(hiddenAliasMetadata, hidden1);
            oneUnspecified.addIndex(hidden2);
            oneUnspecified.addIndex(hidden3);
            oneUnspecified.addIndex(indexWithUnspecifiedAlias);
            IllegalStateException exception = expectThrows(IllegalStateException.class,
                () -> oneUnspecified.computeAndValidateAliasProperties());
            assertThat(exception.getMessage(), containsString("alias [" + hiddenAliasName +
                "] has is_hidden set to true on indices ["));
            assertThat(exception.getMessage(), allOf(containsString(hidden1.getIndex().getName()),
                containsString(hidden2.getIndex().getName()),
                containsString(hidden3.getIndex().getName())));
            assertThat(exception.getMessage(), containsString("but does not have is_hidden set to true on indices [" +
                indexWithUnspecifiedAlias.getIndex().getName() + "]; alias must have the same is_hidden setting on all indices"));
        }

        {
            IndexAbstraction.Alias mostlyVisibleOneHidden;
            if (randomBoolean()) {
                mostlyVisibleOneHidden = new IndexAbstraction.Alias(hiddenAliasMetadata, indexWithNonHiddenAlias);
                mostlyVisibleOneHidden.addIndex(indexWithUnspecifiedAlias);
            } else {
                mostlyVisibleOneHidden = new IndexAbstraction.Alias(hiddenAliasMetadata, indexWithUnspecifiedAlias);
                mostlyVisibleOneHidden.addIndex(indexWithNonHiddenAlias);
            }
            final IndexMetadata hiddenIndex = randomFrom(hidden1, hidden2, hidden3);
            mostlyVisibleOneHidden.addIndex(hiddenIndex);
            IllegalStateException exception = expectThrows(IllegalStateException.class,
                () -> mostlyVisibleOneHidden.computeAndValidateAliasProperties());
            assertThat(exception.getMessage(), containsString("alias [" + hiddenAliasName + "] has is_hidden set to true on " +
                "indices [" + hiddenIndex.getIndex().getName() + "] but does not have is_hidden set to true on indices ["));
            assertThat(exception.getMessage(), allOf(containsString(indexWithUnspecifiedAlias.getIndex().getName()),
                containsString(indexWithNonHiddenAlias.getIndex().getName())));
            assertThat(exception.getMessage(), containsString("but does not have is_hidden set to true on indices ["));
        }
    }

    public void testSystemAliasValidationMixedVersionSystemAndRegularFails() {
        final Version random7xVersion = VersionUtils.randomVersionBetween(random(), Version.V_7_0_0,
            VersionUtils.getPreviousVersion(Version.V_8_0_0));
        final AliasMetadata aliasMetadata = new AliasMetadata.Builder(SYSTEM_ALIAS_NAME).build();
        final IndexMetadata currentVersionSystem = buildIndexWithAlias(".system1", SYSTEM_ALIAS_NAME, null, Version.CURRENT, true);
        final IndexMetadata oldVersionSystem = buildIndexWithAlias(".oldVersionSystem", SYSTEM_ALIAS_NAME, null, random7xVersion, true);
        final IndexMetadata regularIndex = buildIndexWithAlias("regular1", SYSTEM_ALIAS_NAME, false, Version.CURRENT, false);

        IndexAbstraction.Alias mixedVersionSystemAndRegular = new IndexAbstraction.Alias(aliasMetadata, currentVersionSystem);
        mixedVersionSystemAndRegular.addIndex(oldVersionSystem);
        mixedVersionSystemAndRegular.addIndex(regularIndex);
        IllegalStateException exception = expectThrows(IllegalStateException.class,
            () -> mixedVersionSystemAndRegular.computeAndValidateAliasProperties());
        assertThat(exception.getMessage(), containsString("alias [" + SYSTEM_ALIAS_NAME +
            "] refers to both system indices [" + currentVersionSystem.getIndex().getName() + "] and non-system indices: [" +
            regularIndex.getIndex().getName() + "], but aliases must refer to either system or non-system indices, not both"));
    }

    public void testSystemAliasValidationNewSystemAndRegularFails() {
        final AliasMetadata aliasMetadata = new AliasMetadata.Builder(SYSTEM_ALIAS_NAME).build();
        final IndexMetadata currentVersionSystem = buildIndexWithAlias(".system1", SYSTEM_ALIAS_NAME, null, Version.CURRENT, true);
        final IndexMetadata regularIndex = buildIndexWithAlias("regular1", SYSTEM_ALIAS_NAME, false, Version.CURRENT, false);

        IndexAbstraction.Alias systemAndRegular = new IndexAbstraction.Alias(aliasMetadata, currentVersionSystem);
        systemAndRegular.addIndex(regularIndex);
        IllegalStateException exception = expectThrows(IllegalStateException.class,
            () -> systemAndRegular.computeAndValidateAliasProperties());
        assertThat(exception.getMessage(), containsString("alias [" + SYSTEM_ALIAS_NAME +
            "] refers to both system indices [" + currentVersionSystem.getIndex().getName() + "] and non-system indices: [" +
            regularIndex.getIndex().getName() + "], but aliases must refer to either system or non-system indices, not both"));
    }

    public void testSystemAliasOldSystemAndNewRegular() {
        final Version random7xVersion = VersionUtils.randomVersionBetween(random(), Version.V_7_0_0,
            VersionUtils.getPreviousVersion(Version.V_8_0_0));
        final AliasMetadata aliasMetadata = new AliasMetadata.Builder(SYSTEM_ALIAS_NAME).build();
        final IndexMetadata oldVersionSystem = buildIndexWithAlias(".oldVersionSystem", SYSTEM_ALIAS_NAME, null, random7xVersion, true);
        final IndexMetadata regularIndex = buildIndexWithAlias("regular1", SYSTEM_ALIAS_NAME, false, Version.CURRENT, false);

        IndexAbstraction.Alias oldAndRegular = new IndexAbstraction.Alias(aliasMetadata, oldVersionSystem);
        oldAndRegular.addIndex(regularIndex);
        oldAndRegular.computeAndValidateAliasProperties(); // Should be ok
    }

    public void testSystemIndexValidationAllRegular() {
        final Version random7xVersion = VersionUtils.randomVersionBetween(random(), Version.V_7_0_0,
            VersionUtils.getPreviousVersion(Version.V_8_0_0));
        final AliasMetadata aliasMetadata = new AliasMetadata.Builder(SYSTEM_ALIAS_NAME).build();
        final IndexMetadata currentVersionSystem = buildIndexWithAlias(".system1", SYSTEM_ALIAS_NAME, null, Version.CURRENT, true);
        final IndexMetadata currentVersionSystem2 = buildIndexWithAlias(".system2", SYSTEM_ALIAS_NAME, null, Version.CURRENT, true);
        final IndexMetadata oldVersionSystem = buildIndexWithAlias(".oldVersionSystem", SYSTEM_ALIAS_NAME, null, random7xVersion, true);

        IndexAbstraction.Alias allRegular = new IndexAbstraction.Alias(aliasMetadata, currentVersionSystem);
        allRegular.addIndex(currentVersionSystem2);
        allRegular.addIndex(oldVersionSystem);
        allRegular.computeAndValidateAliasProperties(); // Should be ok
    }

    public void testSystemAliasValidationAllSystemSomeOld() {
        final Version random7xVersion = VersionUtils.randomVersionBetween(random(), Version.V_7_0_0,
            VersionUtils.getPreviousVersion(Version.V_8_0_0));
        final AliasMetadata aliasMetadata = new AliasMetadata.Builder(SYSTEM_ALIAS_NAME).build();
        final IndexMetadata currentVersionSystem = buildIndexWithAlias(".system1", SYSTEM_ALIAS_NAME, null, Version.CURRENT, true);
        final IndexMetadata currentVersionSystem2 = buildIndexWithAlias(".system2", SYSTEM_ALIAS_NAME, null, Version.CURRENT, true);
        final IndexMetadata oldVersionSystem = buildIndexWithAlias(".oldVersionSystem", SYSTEM_ALIAS_NAME, null, random7xVersion, true);

        IndexAbstraction.Alias allSystemMixed = new IndexAbstraction.Alias(aliasMetadata, currentVersionSystem);
        allSystemMixed.addIndex(currentVersionSystem2);
        allSystemMixed.addIndex(oldVersionSystem);
        allSystemMixed.computeAndValidateAliasProperties(); // Should be ok
    }

    public void testSystemAliasValidationAll8x() {
        final AliasMetadata aliasMetadata = new AliasMetadata.Builder(SYSTEM_ALIAS_NAME).build();
        final IndexMetadata currentVersionSystem = buildIndexWithAlias(".system1", SYSTEM_ALIAS_NAME, null, Version.CURRENT, true);
        final IndexMetadata currentVersionSystem2 = buildIndexWithAlias(".system2", SYSTEM_ALIAS_NAME, null, Version.CURRENT, true);

        IndexAbstraction.Alias allSystemCurrent = new IndexAbstraction.Alias(aliasMetadata, currentVersionSystem);
        allSystemCurrent.addIndex(currentVersionSystem2);
        allSystemCurrent.computeAndValidateAliasProperties(); // Should be ok
    }

    private IndexMetadata buildIndexWithAlias(String indexName, String aliasName, @Nullable Boolean aliasIsHidden,
                                              Version indexCreationVersion, boolean isSystem) {
        final AliasMetadata.Builder aliasMetadata = new AliasMetadata.Builder(aliasName);
        if (Objects.nonNull(aliasIsHidden) || randomBoolean()) {
            aliasMetadata.isHidden(aliasIsHidden);
        }
        return new IndexMetadata.Builder(indexName)
            .settings(settings(indexCreationVersion))
            .system(isSystem)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(aliasMetadata)
            .build();
    }

}
