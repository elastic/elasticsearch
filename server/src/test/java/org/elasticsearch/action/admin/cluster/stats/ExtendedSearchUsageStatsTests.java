/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ExtendedSearchUsageStatsTests extends AbstractWireSerializingTestCase<ExtendedSearchUsageStats> {

    @Override
    protected Reader<ExtendedSearchUsageStats> instanceReader() {
        return ExtendedSearchUsageStats::new;
    }

    public static ExtendedSearchUsageStats randomExtendedSearchUsage() {
        return randomExtendedSearchUsage(randomBoolean());
    }

    public static ExtendedSearchUsageStats randomExtendedSearchUsage(boolean empty) {
        if (empty) {
            return new ExtendedSearchUsageStats();
        }
        Map<String, Map<String, Map<String, Long>>> categoriesToExtendedData = new HashMap<>();

        // TODO: Gate this behind a randomBoolean() in the future when we have other categories to add.
        categoriesToExtendedData.put("retrievers", randomExtendedRetrieversData());

        return new ExtendedSearchUsageStats(categoriesToExtendedData);
    }

    private static Map<String, Map<String, Long>> randomExtendedRetrieversData() {
        Map<String, Map<String, Long>> retrieversData = new HashMap<>();

        // TODO: Gate this behind a randomBoolean() in the future when we have other values to add.
        Map<String, Long> values = Map.of("chunk_rescorer", randomLongBetween(1, 10));
        retrieversData.put("text_similarity_reranker", values);

        return retrieversData;
    }

    @Override
    protected ExtendedSearchUsageStats createTestInstance() {
        return randomExtendedSearchUsage();
    }

    @Override
    protected ExtendedSearchUsageStats mutateInstance(ExtendedSearchUsageStats instance) throws IOException {
        Map<String, Map<String, Map<String, Long>>> current = instance.getCategoriesToExtendedData();
        Map<String, Map<String, Map<String, Long>>> modified = new HashMap<>();
        if (current.isEmpty()) {
            modified.put("retrievers", Map.of("text_similarity_reranker", Map.of("chunk_rescorer", randomLongBetween(1, 10))));
        } else {
            if (randomBoolean()) {
                modified.put(
                    "retrievers",
                    Map.of(
                        "text_similarity_reranker",
                        Map.of("chunk_rescorer", (Long) randomValueOtherThan(current, () -> randomLongBetween(1, 10)))
                    )
                );
            }
        }
        return new ExtendedSearchUsageStats(modified);
    }

}
