/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;

/**
 * Fixtures for {@code FILLNULL} heap-attack tests. {@code FILLNULL <value> ON *} over N columns expands to a single
 * {@code Eval} of N {@code COALESCE} aliases, which is planned as N chained {@code EvalOperator}s, so the attack vector
 * is the mapped column count rather than the document count.
 */
public abstract class HeapAttackFillNullTestCase extends HeapAttackTestCase {
    protected static final String INDEX = "fillnull_wide";

    /**
     * How the N {@code long} columns are populated. The shape decides which {@code COALESCE} path runs per page:
     * all-null blocks take a constant fast path, mixed blocks are rebuilt position by position.
     */
    protected enum Shape {
        /** Mapped but never written: every column is null in every document. */
        ALL_NULL,
        /** Document {@code d} writes column {@code f} only when {@code f + d} is even, so every page mixes nulls and values. */
        MIXED_NULL
    }

    @Before
    public void checkCapability() {
        assumeTrue("FILLNULL is snapshot-only", EsqlCapabilities.Cap.FILLNULL.isEnabled());
    }

    /**
     * Creates {@link #INDEX} with a {@code sort_key} plus {@code fields} mapped {@code long} columns, raising
     * {@code index.mapping.total_fields.limit} past its default of 1000, and loads {@code docs} documents of {@code shape}.
     * A single shard keeps one data driver per query, so profiles of different column counts stay comparable.
     */
    protected void initWideIndex(int fields, int docs, Shape shape) throws IOException {
        logger.info("loading {} {} documents with {} long columns", docs, shape, fields);
        Request create = new Request("PUT", "/" + INDEX);
        XContentBuilder config = JsonXContent.contentBuilder().startObject();
        config.startObject("settings")
            .field("index.mapping.total_fields.limit", fields + 10)
            .field("index.number_of_shards", 1)
            .endObject();
        config.startObject("mappings").startObject("properties");
        config.startObject("sort_key").field("type", "long").endObject();
        for (int f = 0; f < fields; f++) {
            config.startObject(fieldName(f)).field("type", "long").endObject();
        }
        config.endObject().endObject();
        create.setJsonEntity(Strings.toString(config.endObject()));
        Response response = client().performRequest(create);
        assertThat(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8), containsString("\"acknowledged\":true"));

        // Keep each bulk body around a few MB; a mixed-null document carries fields / 2 values.
        int bytesPerDoc = shape == Shape.MIXED_NULL ? fields * 8 + 32 : 32;
        int docsPerBulk = Math.max(1, Math.min(1000, 4_000_000 / bytesPerDoc));
        StringBuilder bulk = new StringBuilder();
        for (int d = 0; d < docs; d++) {
            bulk.append("{\"create\":{}}\n{\"sort_key\":").append(d);
            if (shape == Shape.MIXED_NULL) {
                for (int f = 0; f < fields; f++) {
                    if (isPopulated(f, d)) {
                        bulk.append(",\"").append(fieldName(f)).append("\":").append(d);
                    }
                }
            }
            bulk.append("}\n");
            if (d % docsPerBulk == docsPerBulk - 1 && d != docs - 1) {
                bulk(INDEX, bulk.toString());
                bulk.setLength(0);
            }
        }
        initIndex(INDEX, bulk.toString());
    }

    /**
     * Sets {@code esql.query.max_fillnull_fields} for this test to fit {@code FILLNULL 0 ON *} over {@code fields} columns, which also
     * fills {@code sort_key}. ESRestTestCase wipes cluster settings after each test.
     */
    protected static void allowFillNullOnAll(int fields) throws IOException {
        Settings settings = Settings.builder().put(EsqlFlags.ESQL_MAX_FILLNULL_FIELDS.getKey(), fields + 1).build();
        updateClusterSettings(adminClient(), settings);
    }

    /** Whether {@link Shape#MIXED_NULL} document {@code doc} has a value for column {@code field}; that value is {@code doc}. */
    protected static boolean isPopulated(int field, int doc) {
        return (field + doc) % 2 == 0;
    }

    protected static String fieldName(int field) {
        return "f" + String.format(Locale.ROOT, "%05d", field);
    }

    /**
     * {@code FROM INDEX [| FILLNULL 0 ON *] | LIMIT limit}, optionally profiled. No {@code SORT}: {@code ON *} rewrites
     * {@code sort_key} into a {@code COALESCE} alias too, which would stop the sort pushing down only in the FILLNULL run.
     */
    protected static String wideQuery(boolean fillNull, int limit, boolean profile) {
        StringBuilder query = startQuery();
        query.append("FROM ").append(INDEX).append("\n");
        if (fillNull) {
            query.append("| FILLNULL 0 ON *\n");
        }
        query.append("| LIMIT ").append(limit).append('"');
        if (profile) {
            query.append(",\"profile\":true");
        }
        return query.append('}').toString();
    }

    /**
     * {@code FROM INDEX | FILLNULL 0 ON target | SORT sortField | LIMIT limit}: sorting on a filled column keeps the COALESCEs in the
     * data driver, the costliest place for them.
     */
    protected static String sortedFillNullQuery(String target, String sortField, int limit, boolean profile) {
        StringBuilder query = startQuery().append("FROM ")
            .append(INDEX)
            .append(" | FILLNULL 0 ON ")
            .append(target)
            .append(" | SORT ")
            .append(sortField)
            .append(" | LIMIT ")
            .append(limit)
            .append('"');
        if (profile) {
            query.append(",\"profile\":true");
        }
        return query.append('}').toString();
    }
}
