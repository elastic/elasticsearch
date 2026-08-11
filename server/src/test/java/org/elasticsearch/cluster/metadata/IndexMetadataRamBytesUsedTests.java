/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.test.AbstractAccountableFieldsTestCase;

import java.util.Set;

/**
 * Structural tripwire for {@link IndexMetadata#ramBytesUsed()}. Behavioural assertions (that populating a field increases the reported
 * size) live in {@link IndexMetadataTests}.
 */
public class IndexMetadataRamBytesUsedTests extends AbstractAccountableFieldsTestCase {

    @Override
    protected Class<? extends Accountable> classUnderTest() {
        return IndexMetadata.class;
    }

    @Override
    protected Set<String> fieldsAccountedForInRamBytesUsed() {
        return Set.of(
            "index",
            "settings",
            "mapping",
            "primaryTerms",
            "inSyncAllocationIds",
            "aliases",
            "customData",
            "inferenceFields",
            "rolloverInfos",
            "transportVersion",
            "state",
            "routingPaths",
            "timeSeriesDimensions",
            "requireFilters",
            "includeFilters",
            "excludeFilters",
            "initialRecoveryFilters",
            "indexCreatedVersion",
            "mappingsUpdatedVersion",
            "indexCompatibilityVersion",
            "waitForActiveShards",
            "timestampRange",
            "eventIngestedRange",
            "tierPreference",
            "lifecyclePolicyName",
            "lifecycleExecutionState",
            "autoExpandReplicas",
            "indexMode",
            "timeSeriesStart",
            "timeSeriesEnd",
            "stats",
            "writeLoadForecast",
            "shardSizeInBytesForecast",
            "reshardingMetadata"
        );
    }

    @Override
    protected Set<String> fieldsExcludedFromRamBytesUsed() {
        // The only non-primitive field not contributing its own heap cost is the memoization cache, which is a primitive long and is
        // therefore already ignored by the base class. All remaining unaccounted fields are primitives (shard counts, versions, flags),
        // so there is nothing to exclude explicitly here.
        return Set.of();
    }

    @Override
    protected boolean assertsAgainstRamUsageTester() {
        // Settings.estimatedRamBytesUsed() deliberately omits interned keys/values; a full-graph RamUsageTester walk would count them
        // and fail estimate >= actual by design. Structural + behavioural checks in IndexMetadataTests cover this class instead.
        return false;
    }
}
