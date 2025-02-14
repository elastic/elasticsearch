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
import java.util.List;
import java.util.Map;

/**
 * Challenge test that uses bulk indexing for both baseline and contender sides.
 * We index same documents into an index with standard index mode and an index with logsdb index mode.
 * Then we verify that results of common operations are the same modulo knows differences like synthetic source
 * modifications.
 */
public class BulkChallengeRestIT extends StandardVersusLogsIndexModeChallengeRestIT {

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

    private Map<String, Object> indexContenderDocuments(final CheckedSupplier<List<XContentBuilder>, IOException> documentsSupplier)
        throws IOException {
        final StringBuilder sb = new StringBuilder();
        int id = 0;
        for (var document : documentsSupplier.get()) {
            if (autoGenerateId()) {
                sb.append("{ \"create\": { } }\n");
            } else {
                sb.append(Strings.format("{ \"create\": { \"_id\" : \"%d\" } }\n", id));
            }
            sb.append(Strings.toString(document)).append("\n");
            id++;
        }
        return performBulkRequest(sb.toString(), false);
    }

    @SuppressWarnings("unchecked")
    private void indexBaselineDocuments(
        final CheckedSupplier<List<XContentBuilder>, IOException> documentsSupplier,
        final Map<String, Object> contenderResponseEntity
    ) throws IOException {
        final StringBuilder sb = new StringBuilder();
        int id = 0;
        final List<Map<String, Object>> items = (List<Map<String, Object>>) contenderResponseEntity.get("items");
        for (var document : documentsSupplier.get()) {
            if (autoGenerateId()) {
                var contenderId = ((Map<String, Object>) items.get(id).get("create")).get("_id");
                sb.append(Strings.format("{ \"create\": { \"_id\" : \"%s\" } }\n", contenderId));
            } else {
                sb.append(Strings.format("{ \"create\": { \"_id\" : \"%d\" } }\n", id));
            }
            sb.append(Strings.toString(document)).append("\n");
            id++;
        }
        performBulkRequest(sb.toString(), true);
    }
}
