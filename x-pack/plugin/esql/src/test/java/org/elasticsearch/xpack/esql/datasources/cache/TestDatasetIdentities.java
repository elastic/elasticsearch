/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import java.util.Map;

/**
 * Builds a {@link DatasetIdentity} from the pieces these suites used to pass to the key directly, so a test that
 * varied the format name or the folded participant identity still varies the same thing.
 * <p>
 * It mirrors what {@code ExternalSourceResolver#datasetIdentity} folds, with the slots a unit test can reach:
 * the definition version read out of the config, the folded participant identity in the storage slot, and a
 * caller-chosen string in the reader slot. The secret digest is empty here, which is what a provider that
 * consumed no declared secret produces - a suite that wants to vary it passes it explicitly.
 * <p>
 * The reader slot is NOT where a format name reaches a key in production: a reader's identity renders only
 * the recognized settings its config carries. A suite passing a format name here varies the reader lane by
 * hand, which is enough to prove two identities differ but does not exercise how production separates two
 * formats - that is the coordinator lane, and
 * {@code ExternalSourceResolverTests#testDatasetIdentitySeparatesTwoFormatsOverOneObject} covers it.
 */
public final class TestDatasetIdentities {

    private TestDatasetIdentities() {}

    public static DatasetIdentity identity(String formatType, String foldedIdentity, Map<String, Object> config) {
        return identity(formatType, foldedIdentity, "", config);
    }

    public static DatasetIdentity identity(String formatType, String foldedIdentity, String secretIdentity, Map<String, Object> config) {
        return DatasetIdentity.of(DatasetIdentity.definitionVersionOf(config), secretIdentity, foldedIdentity, formatType, "");
    }
}
