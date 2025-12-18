/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.qa;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

/**
 * Challenge test that uses bulk indexing for both baseline and contender sides.
 * We index same documents into an index with standard index mode and an index with logsdb index mode.
 * Then we verify that results of common operations are the same modulo knows differences like synthetic source
 * modifications.
 */
public class BulkChallengeRestIT extends StandardVersusLogsIndexModeChallengeRestIT {

    private static final int BULK_BATCH_SIZE = 20;

    public BulkChallengeRestIT() {}

    protected BulkChallengeRestIT(DataGenerationHelper dataGenerationHelper) {
        super(dataGenerationHelper);
    }

    @Override
    public void indexDocuments(
        final CheckedSupplier<List<XContentBuilder>, IOException> baselineSupplier,
        final CheckedSupplier<List<XContentBuilder>, IOException> contenderSupplier
    ) throws IOException {
        var contenderResponseEntity = indexContenderDocuments(contenderSupplier);
        indexBaselineDocuments(baselineSupplier, contenderResponseEntity);
    }

    private List<Map<String, Object>> indexContenderDocuments(final CheckedSupplier<List<XContentBuilder>, IOException> documentsSupplier)
        throws IOException {
        final IntFunction<String> bulkActionGenerator = id -> autoGenerateId()
            ? "{ \"create\": { } }\n"
            : Strings.format("{ \"create\": { \"_id\" : \"%d\" } }\n", id);

        return indexDocumentsInBatches(documentsSupplier.get(), bulkActionGenerator, false);
    }

    @SuppressWarnings("unchecked")
    private void indexBaselineDocuments(
        final CheckedSupplier<List<XContentBuilder>, IOException> documentsSupplier,
        final List<Map<String, Object>> contenderItems
    ) throws IOException {
        final IntFunction<String> bulkActionGenerator = id -> {
            if (autoGenerateId()) {
                final var contenderId = ((Map<String, Object>) contenderItems.get(id).get("create")).get("_id");
                return Strings.format("{ \"create\": { \"_id\" : \"%s\" } }\n", contenderId);
            } else {
                return Strings.format("{ \"create\": { \"_id\" : \"%d\" } }\n", id);
            }
        };

        indexDocumentsInBatches(documentsSupplier.get(), bulkActionGenerator, true);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> indexDocumentsInBatches(
        final List<XContentBuilder> documents,
        final IntFunction<String> bulkActionGenerator,
        final boolean isBaseline
    ) throws IOException {
        final List<Map<String, Object>> allItems = new ArrayList<>();
        final StringBuilder sb = new StringBuilder();
        int id = 0;
        int batchCount = 0;

        for (final var document : documents) {
            sb.append(bulkActionGenerator.apply(id));
            sb.append(Strings.toString(document)).append("\n");
            id++;
            batchCount++;

            if (batchCount >= BULK_BATCH_SIZE) {
                final Map<String, Object> response = performBulkRequest(sb.toString(), isBaseline);
                allItems.addAll((List<Map<String, Object>>) response.get("items"));
                sb.setLength(0);
                batchCount = 0;
            }
        }

        if (batchCount > 0) {
            final Map<String, Object> response = performBulkRequest(sb.toString(), isBaseline);
            allItems.addAll((List<Map<String, Object>>) response.get("items"));
        }

        return allItems;
    }
}
