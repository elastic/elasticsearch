/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spatial;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressCodecs;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlPluginWithEnterpriseOrTrialLicense;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;
import org.elasticsearch.xpack.spatial.SpatialPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

/**
 * A {@code SORT}/{@code LIMIT} makes {@code LuceneTopNSourceOperator} emit pages in sort order, so a later page can start behind a
 * document an earlier page already read. {@code ValuesSourceReaderOperator} keeps a column reader across those pages unless
 * {@link org.elasticsearch.index.mapper.BlockLoader.ColumnAtATimeReader#canReuse} says otherwise, and the {@code shape} readers hold a
 * forward-only binary doc values iterator. When the field is stored in the TSDB doc values format its values are chopped into
 * compressed blocks, so revisiting an earlier document resolved against the still-loaded later block and threw
 * {@link ArrayIndexOutOfBoundsException} for a negative block-relative index.
 * <p>
 * {@code @SuppressCodecs("*")} matters: {@link org.elasticsearch.test.ESIntegTestCase} otherwise forces
 * {@code index.codec=lucene_default} through its random index template, which bypasses {@code PerFieldFormatSupplier} and therefore
 * never selects the TSDB doc values format these readers need in order to reproduce the failure.
 */
@SuppressCodecs("*")
public class ShapeBackwardsReadIT extends AbstractEsqlIntegTestCase {

    /** Enough documents, with fat enough geometries, to spill the binary doc values across several compressed blocks. */
    private static final int NUM_DOCS = 6000;
    private static final int VERTICES = 60;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(SpatialPlugin.class, TestEncryptionServicePlugin.class, EsqlPluginWithEnterpriseOrTrialLicense.class);
    }

    /**
     * {@code columnar} index mode enables the TSDB doc values format by default and routes {@code shape} value reads through
     * {@code GeometrySourceBlockLoader}, so a plain {@code KEEP} after a {@code SORT} is enough.
     */
    public void testShapeReadsAfterTopNOnColumnarIndex() throws Exception {
        String index = "shape_columnar";
        createShapeIndex(index, Settings.builder().put("index.mode", "columnar").put("index.codec", "default"));

        assertQueriesSucceed(
            index,
            "SORT sort_key | LIMIT 5000 | KEEP location",
            "SORT sort_key DESC | LIMIT 5000 | KEEP location",
            "SORT sort_key | LIMIT 5000 | EVAL wkt = TO_STRING(location) | KEEP wkt",
            "SORT sort_key | LIMIT 5000 | STATS extent = ST_EXTENT_AGG(location)",
            "SORT sort_key | LIMIT 5000 | STATS centroid = ST_CENTROID_AGG(location)",
            "SORT sort_key | LIMIT 5000 | STATS e = ST_EXTENT_AGG(location), c = ST_CENTROID_AGG(location)"
        );
    }

    /**
     * Runs every query and reports all of them, rather than stopping at the first, so a partial regression cannot hide behind an
     * earlier failure.
     */
    private void assertQueriesSucceed(String index, String... tails) {
        List<String> failures = new ArrayList<>();
        for (String tail : tails) {
            String query = "FROM " + index + " | " + tail;
            try (var resp = run(query)) {
                assertThat(resp.columns(), not(empty()));
            } catch (Exception e) {
                Throwable root = e;
                while (root.getCause() != null) {
                    root = root.getCause();
                }
                failures.add(tail + " -> " + root.getClass().getSimpleName() + ": " + root.getMessage());
            }
        }
        assertThat("queries must not fail on a reused reader", failures, empty());
    }

    private void createShapeIndex(String index, Settings.Builder settings) throws Exception {
        assertAcked(prepareCreate(index).setSettings(settings.put("index.number_of_shards", 1).build()).setMapping("""
            {
              "properties" : {
                "location": { "type" : "shape" },
                "sort_key": { "type" : "long" }
              }
            }
            """));

        List<IndexRequest> batch = new ArrayList<>();
        for (int i = 0; i < NUM_DOCS; i++) {
            batch.add(new IndexRequest(index).id(Integer.toString(i)).source("location", polygon(i), "sort_key", scrambled(i)));
            if (batch.size() == 500) {
                indexBatch(batch);
                batch = new ArrayList<>();
            }
        }
        if (batch.isEmpty() == false) {
            indexBatch(batch);
        }
        // One segment, so the reader is retained across pages instead of being dropped at a segment boundary.
        client().admin().indices().prepareForceMerge(index).setMaxNumSegments(1).get();
        client().admin().indices().prepareRefresh(index).get();
        ensureYellow(index);
    }

    private void indexBatch(List<IndexRequest> batch) {
        BulkRequestBuilder bulk = client().prepareBulk();
        batch.forEach(bulk::add);
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE).get();
    }

    /** Uncorrelated with the document id, so sorting on it genuinely shuffles documents across pages. */
    private static long scrambled(int i) {
        return (i * 2654435761L) % 1000003L;
    }

    private static String polygon(int i) {
        double cx = i % 100;
        double cy = i % 50;
        double r = 0.4;
        StringBuilder sb = new StringBuilder("POLYGON((");
        for (int v = 0; v <= VERTICES; v++) {
            double angle = 2 * Math.PI * (v % VERTICES) / VERTICES;
            if (v > 0) {
                sb.append(", ");
            }
            sb.append(String.format(Locale.ROOT, "%.6f %.6f", cx + r * Math.cos(angle), cy + r * Math.sin(angle)));
        }
        return sb.append("))").toString();
    }
}
