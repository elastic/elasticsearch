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

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

public class IndexAbstractionTests extends ESTestCase {

    public static final String SYSTEM_ALIAS_NAME = "system_alias";

    public void testHiddenAliasValidation() {
        final String hiddenAliasName = "hidden_alias";

        IndexMetadata hidden1 = buildIndexWithAlias("hidden1", hiddenAliasName, true, Version.CURRENT, false);
        IndexMetadata hidden2 = buildIndexWithAlias("hidden2", hiddenAliasName, true, Version.CURRENT, false);
        IndexMetadata hidden3 = buildIndexWithAlias("hidden3", hiddenAliasName, true, Version.CURRENT, false);

        IndexMetadata nonHidden = buildIndexWithAlias("nonhidden1", hiddenAliasName, false, Version.CURRENT, false);
        IndexMetadata unspecified = buildIndexWithAlias("nonhidden2", hiddenAliasName, null, Version.CURRENT, false);

        {
            // Should be ok:
            metadataWithIndices(hidden1, hidden2, hidden3);
        }

        {
            // Should be ok:
            if (randomBoolean()) {
                metadataWithIndices(nonHidden, unspecified);
            } else {
                metadataWithIndices(unspecified, nonHidden);
            }
        }

        {
            IllegalStateException exception = expectThrows(
                IllegalStateException.class,
                () -> metadataWithIndices(hidden1, hidden2, hidden3, nonHidden)
            );
            assertThat(exception.getMessage(), containsString("alias [" + hiddenAliasName + "] has is_hidden set to true on indices ["));
            assertThat(
                exception.getMessage(),
                allOf(
                    containsString(hidden1.getIndex().getName()),
                    containsString(hidden2.getIndex().getName()),
                    containsString(hidden3.getIndex().getName())
                )
            );
            assertThat(
                exception.getMessage(),
                containsString(
                    "but does not have is_hidden set to true on indices ["
                        + nonHidden.getIndex().getName()
                        + "]; alias must have the same is_hidden setting on all indices"
                )
            );
        }

        {
            IllegalStateException exception = expectThrows(
                IllegalStateException.class,
                () -> metadataWithIndices(hidden1, hidden2, hidden3, unspecified)
            );
            assertThat(exception.getMessage(), containsString("alias [" + hiddenAliasName + "] has is_hidden set to true on indices ["));
            assertThat(
                exception.getMessage(),
                allOf(
                    containsString(hidden1.getIndex().getName()),
                    containsString(hidden2.getIndex().getName()),
                    containsString(hidden3.getIndex().getName())
                )
            );
            assertThat(
                exception.getMessage(),
                containsString(
                    "but does not have is_hidden set to true on indices ["
                        + unspecified.getIndex().getName()
                        + "]; alias must have the same is_hidden setting on all indices"
                )
            );
        }

        {
            final IndexMetadata hiddenIndex = randomFrom(hidden1, hidden2, hidden3);
            IllegalStateException exception = expectThrows(IllegalStateException.class, () -> {
                if (randomBoolean()) {
                    metadataWithIndices(nonHidden, unspecified, hiddenIndex);
                } else {
                    metadataWithIndices(unspecified, nonHidden, hiddenIndex);
                }
            });
            assertThat(
                exception.getMessage(),
                containsString(
                    "alias ["
                        + hiddenAliasName
                        + "] has is_hidden set to true on "
                        + "indices ["
                        + hiddenIndex.getIndex().getName()
                        + "] but does not have is_hidden set to true on indices ["
                )
            );
            assertThat(
                exception.getMessage(),
                allOf(containsString(unspecified.getIndex().getName()), containsString(nonHidden.getIndex().getName()))
            );
            assertThat(exception.getMessage(), containsString("but does not have is_hidden set to true on indices ["));
        }
    }

    public void testSystemAliasValidationMixedVersionSystemAndRegularFails() {
        final Version random7xVersion = VersionUtils.randomVersionBetween(
            random(),
            Version.V_7_0_0,
            VersionUtils.getPreviousVersion(Version.V_8_0_0)
        );
        final IndexMetadata currentVersionSystem = buildIndexWithAlias(".system1", SYSTEM_ALIAS_NAME, null, Version.CURRENT, true);
        final IndexMetadata oldVersionSystem = buildIndexWithAlias(".oldVersionSystem", SYSTEM_ALIAS_NAME, null, random7xVersion, true);
        final IndexMetadata regularIndex = buildIndexWithAlias("regular1", SYSTEM_ALIAS_NAME, false, Version.CURRENT, false);

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> metadataWithIndices(currentVersionSystem, oldVersionSystem, regularIndex)
        );
        assertThat(
            exception.getMessage(),
            containsString(
                "alias ["
                    + SYSTEM_ALIAS_NAME
                    + "] refers to both system indices ["
                    + currentVersionSystem.getIndex().getName()
                    + "] and non-system indices: ["
                    + regularIndex.getIndex().getName()
                    + "], but aliases must refer to either system or non-system indices, not both"
            )
        );
    }

    public void testSystemAliasValidationNewSystemAndRegularFails() {
        final IndexMetadata currentVersionSystem = buildIndexWithAlias(".system1", SYSTEM_ALIAS_NAME, null, Version.CURRENT, true);
        final IndexMetadata regularIndex = buildIndexWithAlias("regular1", SYSTEM_ALIAS_NAME, false, Version.CURRENT, false);

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> metadataWithIndices(currentVersionSystem, regularIndex)
        );
        assertThat(
            exception.getMessage(),
            containsString(
                "alias ["
                    + SYSTEM_ALIAS_NAME
                    + "] refers to both system indices ["
                    + currentVersionSystem.getIndex().getName()
                    + "] and non-system indices: ["
                    + regularIndex.getIndex().getName()
                    + "], but aliases must refer to either system or non-system indices, not both"
            )
        );
    }

    public void testSystemAliasOldSystemAndNewRegular() {
        final Version random7xVersion = VersionUtils.randomVersionBetween(
            random(),
            Version.V_7_0_0,
            VersionUtils.getPreviousVersion(Version.V_8_0_0)
        );
        final IndexMetadata oldVersionSystem = buildIndexWithAlias(".oldVersionSystem", SYSTEM_ALIAS_NAME, null, random7xVersion, true);
        final IndexMetadata regularIndex = buildIndexWithAlias("regular1", SYSTEM_ALIAS_NAME, false, Version.CURRENT, false);

        // Should be ok:
        metadataWithIndices(oldVersionSystem, regularIndex);
    }

    public void testSystemIndexValidationAllRegular() {
        final Version random7xVersion = VersionUtils.randomVersionBetween(
            random(),
            Version.V_7_0_0,
            VersionUtils.getPreviousVersion(Version.V_8_0_0)
        );
        final IndexMetadata currentVersionSystem = buildIndexWithAlias(".system1", SYSTEM_ALIAS_NAME, null, Version.CURRENT, true);
        final IndexMetadata currentVersionSystem2 = buildIndexWithAlias(".system2", SYSTEM_ALIAS_NAME, null, Version.CURRENT, true);
        final IndexMetadata oldVersionSystem = buildIndexWithAlias(".oldVersionSystem", SYSTEM_ALIAS_NAME, null, random7xVersion, true);

        // Should be ok
        metadataWithIndices(currentVersionSystem, currentVersionSystem2, oldVersionSystem);
    }

    public void testSystemAliasValidationAllSystemSomeOld() {
        final Version random7xVersion = VersionUtils.randomVersionBetween(
            random(),
            Version.V_7_0_0,
            VersionUtils.getPreviousVersion(Version.V_8_0_0)
        );
        final IndexMetadata currentVersionSystem = buildIndexWithAlias(".system1", SYSTEM_ALIAS_NAME, null, Version.CURRENT, true);
        final IndexMetadata currentVersionSystem2 = buildIndexWithAlias(".system2", SYSTEM_ALIAS_NAME, null, Version.CURRENT, true);
        final IndexMetadata oldVersionSystem = buildIndexWithAlias(".oldVersionSystem", SYSTEM_ALIAS_NAME, null, random7xVersion, true);

        // Should be ok:
        metadataWithIndices(currentVersionSystem, currentVersionSystem2, oldVersionSystem);
    }

    public void testSystemAliasValidationAll8x() {
        final IndexMetadata currentVersionSystem = buildIndexWithAlias(".system1", SYSTEM_ALIAS_NAME, null, Version.CURRENT, true);
        final IndexMetadata currentVersionSystem2 = buildIndexWithAlias(".system2", SYSTEM_ALIAS_NAME, null, Version.CURRENT, true);

        // Should be ok
        metadataWithIndices(currentVersionSystem, currentVersionSystem2);
    }

    private void metadataWithIndices(IndexMetadata... indices) {
        Metadata.Builder builder = Metadata.builder();
        for (var cursor : indices) {
            builder.put(cursor, false);
        }
        builder.build();
    }

    private IndexMetadata buildIndexWithAlias(
        String indexName,
        String aliasName,
        @Nullable Boolean aliasIsHidden,
        Version indexCreationVersion,
        boolean isSystem
    ) {
        final AliasMetadata.Builder aliasMetadata = new AliasMetadata.Builder(aliasName);
        if (aliasIsHidden != null || randomBoolean()) {
            aliasMetadata.isHidden(aliasIsHidden);
        }
        return new IndexMetadata.Builder(indexName).settings(settings(indexCreationVersion))
            .system(isSystem)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(aliasMetadata)
            .build();
    }

}
