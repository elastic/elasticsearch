/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.util.List;
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
            // Should be ok:
            IndexAbstraction.Alias allHidden = new IndexAbstraction.Alias(hiddenAliasMetadata, List.of(hidden1, hidden2, hidden3));
        }

        {
            // Should be ok:
            IndexAbstraction.Alias allVisible;
            if (randomBoolean()) {
                allVisible = new IndexAbstraction.Alias(hiddenAliasMetadata, List.of(indexWithNonHiddenAlias, indexWithUnspecifiedAlias));
            } else {
                allVisible = new IndexAbstraction.Alias(hiddenAliasMetadata, List.of(indexWithUnspecifiedAlias, indexWithNonHiddenAlias));
            }
        }

        {
            IllegalStateException exception = expectThrows(IllegalStateException.class,
                () -> new IndexAbstraction.Alias(hiddenAliasMetadata, List.of(hidden1, hidden2, hidden3, indexWithNonHiddenAlias)));
            assertThat(exception.getMessage(), containsString("alias [" + hiddenAliasName +
                "] has is_hidden set to true on indices ["));
            assertThat(exception.getMessage(), allOf(containsString(hidden1.getIndex().getName()),
                containsString(hidden2.getIndex().getName()),
                containsString(hidden3.getIndex().getName())));
            assertThat(exception.getMessage(), containsString("but does not have is_hidden set to true on indices [" +
                indexWithNonHiddenAlias.getIndex().getName() + "]; alias must have the same is_hidden setting on all indices"));
        }

        {
            IllegalStateException exception = expectThrows(IllegalStateException.class,
                () -> new IndexAbstraction.Alias(hiddenAliasMetadata, List.of(hidden1, hidden2, hidden3, indexWithUnspecifiedAlias)));
            assertThat(exception.getMessage(), containsString("alias [" + hiddenAliasName +
                "] has is_hidden set to true on indices ["));
            assertThat(exception.getMessage(), allOf(containsString(hidden1.getIndex().getName()),
                containsString(hidden2.getIndex().getName()),
                containsString(hidden3.getIndex().getName())));
            assertThat(exception.getMessage(), containsString("but does not have is_hidden set to true on indices [" +
                indexWithUnspecifiedAlias.getIndex().getName() + "]; alias must have the same is_hidden setting on all indices"));
        }

        {
            final IndexMetadata hiddenIndex = randomFrom(hidden1, hidden2, hidden3);
            IllegalStateException exception = expectThrows(IllegalStateException.class,
                () -> {
                    if (randomBoolean()) {
                        new IndexAbstraction.Alias(hiddenAliasMetadata,
                            List.of(indexWithNonHiddenAlias, indexWithUnspecifiedAlias, hiddenIndex));
                    } else {
                        new IndexAbstraction.Alias(hiddenAliasMetadata,
                            List.of(indexWithUnspecifiedAlias, indexWithNonHiddenAlias, hiddenIndex));
                    }
                });
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

        IllegalStateException exception = expectThrows(IllegalStateException.class,
            () -> new IndexAbstraction.Alias(aliasMetadata, List.of(currentVersionSystem, oldVersionSystem, regularIndex)));
        assertThat(exception.getMessage(), containsString("alias [" + SYSTEM_ALIAS_NAME +
            "] refers to both system indices [" + currentVersionSystem.getIndex().getName() + "] and non-system indices: [" +
            regularIndex.getIndex().getName() + "], but aliases must refer to either system or non-system indices, not both"));
    }

    public void testSystemAliasValidationNewSystemAndRegularFails() {
        final AliasMetadata aliasMetadata = new AliasMetadata.Builder(SYSTEM_ALIAS_NAME).build();
        final IndexMetadata currentVersionSystem = buildIndexWithAlias(".system1", SYSTEM_ALIAS_NAME, null, Version.CURRENT, true);
        final IndexMetadata regularIndex = buildIndexWithAlias("regular1", SYSTEM_ALIAS_NAME, false, Version.CURRENT, false);

        IllegalStateException exception = expectThrows(IllegalStateException.class,
            () -> new IndexAbstraction.Alias(aliasMetadata, List.of(currentVersionSystem, regularIndex)));
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

        // Should be ok:
        new IndexAbstraction.Alias(aliasMetadata, List.of(oldVersionSystem, regularIndex));
    }

    public void testSystemIndexValidationAllRegular() {
        final Version random7xVersion = VersionUtils.randomVersionBetween(random(), Version.V_7_0_0,
            VersionUtils.getPreviousVersion(Version.V_8_0_0));
        final AliasMetadata aliasMetadata = new AliasMetadata.Builder(SYSTEM_ALIAS_NAME).build();
        final IndexMetadata currentVersionSystem = buildIndexWithAlias(".system1", SYSTEM_ALIAS_NAME, null, Version.CURRENT, true);
        final IndexMetadata currentVersionSystem2 = buildIndexWithAlias(".system2", SYSTEM_ALIAS_NAME, null, Version.CURRENT, true);
        final IndexMetadata oldVersionSystem = buildIndexWithAlias(".oldVersionSystem", SYSTEM_ALIAS_NAME, null, random7xVersion, true);

        // Should be ok
        new IndexAbstraction.Alias(aliasMetadata, List.of(currentVersionSystem, currentVersionSystem2, oldVersionSystem));
    }

    public void testSystemAliasValidationAllSystemSomeOld() {
        final Version random7xVersion = VersionUtils.randomVersionBetween(random(), Version.V_7_0_0,
            VersionUtils.getPreviousVersion(Version.V_8_0_0));
        final AliasMetadata aliasMetadata = new AliasMetadata.Builder(SYSTEM_ALIAS_NAME).build();
        final IndexMetadata currentVersionSystem = buildIndexWithAlias(".system1", SYSTEM_ALIAS_NAME, null, Version.CURRENT, true);
        final IndexMetadata currentVersionSystem2 = buildIndexWithAlias(".system2", SYSTEM_ALIAS_NAME, null, Version.CURRENT, true);
        final IndexMetadata oldVersionSystem = buildIndexWithAlias(".oldVersionSystem", SYSTEM_ALIAS_NAME, null, random7xVersion, true);

        // Should be ok:
        new IndexAbstraction.Alias(aliasMetadata, List.of(currentVersionSystem, currentVersionSystem2, oldVersionSystem));
    }

    public void testSystemAliasValidationAll8x() {
        final AliasMetadata aliasMetadata = new AliasMetadata.Builder(SYSTEM_ALIAS_NAME).build();
        final IndexMetadata currentVersionSystem = buildIndexWithAlias(".system1", SYSTEM_ALIAS_NAME, null, Version.CURRENT, true);
        final IndexMetadata currentVersionSystem2 = buildIndexWithAlias(".system2", SYSTEM_ALIAS_NAME, null, Version.CURRENT, true);

        // Should be ok
        new IndexAbstraction.Alias(aliasMetadata, List.of(currentVersionSystem, currentVersionSystem2));
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
