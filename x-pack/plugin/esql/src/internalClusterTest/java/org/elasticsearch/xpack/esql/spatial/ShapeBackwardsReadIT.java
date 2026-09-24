/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spatial;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlPluginWithEnterpriseOrTrialLicense;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.spatial.SpatialPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;

/**
 * A {@code SORT}/{@code LIMIT} makes {@code LuceneTopNSourceOperator} emit pages in sort order, so a later page can start behind a
 * document an earlier page already read. {@code ValuesSourceReaderOperator} keeps a column reader across those pages unless
 * {@link org.elasticsearch.index.mapper.BlockLoader.ColumnAtATimeReader#canReuse} says otherwise, and the {@code shape} readers hold a
 * forward-only binary doc values iterator. When the field is stored in the TSDB doc values format its values are chopped into
 * compressed blocks, so revisiting an earlier document resolved against the still-loaded later block and threw
 * {@link ArrayIndexOutOfBoundsException} for a negative block-relative index.
 * <p>
 * Three preconditions are load-bearing and each is asserted rather than assumed, because if any silently fails the queries all pass
 * while never reaching {@code canReuse}:
 * <ul>
 *     <li>{@code index.codec} must be a real Elasticsearch codec. {@link org.elasticsearch.test.ESIntegTestCase} puts
 *     {@code index.codec=lucene_default} on its random index template, which bypasses {@code PerFieldFormatSupplier} and so never
 *     selects the TSDB doc values format. The create-request setting below overrides the template
 *     ({@code MetadataCreateIndexService} applies request settings last).</li>
 *     <li>{@code page_size} must be well below the query {@code LIMIT}, or the whole TopN result arrives as one page and no reader is
 *     ever handed a document it has passed. {@link #getPragmas()} pins it; {@code randomPragmas()} can otherwise pick a page size
 *     larger than the limit.</li>
 *     <li>The index must be one segment, or {@code ValuesSourceReaderOperator} takes the many-segments path, which consults
 *     {@code positionFieldWorkDocGuaranteedAscending} and never calls {@code canReuse} at all.</li>
 * </ul>
 * <p>
 * If this test fails, expect the unhelpful message {@code AssertionError: timeout} rather than a description of the backwards read.
 * Assertions are enabled on test nodes, so the reader's order assert (or the one in {@code AbstractTSDBDocValuesProducer}) fires first,
 * and an {@link AssertionError} is an {@link Error}: nothing in {@code compute/operator} catches {@link Throwable}, so it escapes to the
 * thread pool's uncaught handler and the query's listener never completes. Re-run with {@code -Dtests.asserts=false} to see the
 * underlying {@link ArrayIndexOutOfBoundsException} and its negative index, which names the block and the document.
 */
public class ShapeBackwardsReadIT extends AbstractEsqlIntegTestCase {

    private static final int NUM_DOCS = 6000;
    private static final int LIMIT = 5000;
    private static final int VERTICES = 60;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(SpatialPlugin.class, TestEncryptionServicePlugin.class, EsqlPluginWithEnterpriseOrTrialLicense.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        // Must stay well under LIMIT so the TopN result spans several pages; that is what lets a reader be reused for an earlier doc.
        return new QueryPragmas(Settings.builder().put(QueryPragmas.PAGE_SIZE.getKey(), 512).build());
    }

    /**
     * One query per affected reader. {@code columnar} index mode enables the TSDB doc values format by default and routes {@code shape}
     * value reads through {@code GeometrySourceBlockLoader}, so a plain {@code KEEP} after a {@code SORT} reaches it.
     */
    public void testShapeReadsAfterTopNOnColumnarIndex() throws Exception {
        String index = "shape_columnar";
        createShapeIndex(index);

        // GeometrySourceReader: also checks the values, so a silent wrong-document read cannot pass.
        assertDistinctGeometries(index, "SORT sort_key | LIMIT " + LIMIT + " | KEEP location");
        // BoundsReader, CentroidReader, BoundsAndCentroidReader.
        assertRows(index, "SORT sort_key | LIMIT " + LIMIT + " | STATS extent = ST_EXTENT_AGG(location)", 1);
        assertRows(index, "SORT sort_key | LIMIT " + LIMIT + " | STATS centroid = ST_CENTROID_AGG(location)", 1);
        assertRows(index, "SORT sort_key | LIMIT " + LIMIT + " | STATS e = ST_EXTENT_AGG(location), c = ST_CENTROID_AGG(location)", 1);
    }

    private void assertRows(String index, String tail, int expectedRows) {
        String query = "FROM " + index + " | " + tail;
        List<List<Object>> values;
        try (var resp = run(query)) {
            values = getValuesList(resp.values());
        }
        assertThat(query, values.size(), equalTo(expectedRows));
    }

    /**
     * Every document carries a distinct geometry, so reading the wrong document shows up as a duplicate even when nothing throws --
     * the silent manifestation of a backwards read, which is what happens on formats that address binary doc values randomly.
     */
    private void assertDistinctGeometries(String index, String tail) {
        String query = "FROM " + index + " | " + tail;
        List<List<Object>> values;
        try (var resp = run(query)) {
            values = getValuesList(resp.values());
        }
        assertThat(query, values.size(), equalTo(LIMIT));
        Set<Object> distinct = new HashSet<>();
        for (List<Object> row : values) {
            distinct.add(row.getFirst());
        }
        assertThat(query + " returned a document's geometry more than once", distinct.size(), equalTo(LIMIT));
    }

    private void createShapeIndex(String index) throws Exception {
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder()
                    .put("index.mode", "columnar")
                    // See the class javadoc: the random index template would otherwise pin lucene_default.
                    .put("index.codec", "default")
                    .put("index.number_of_shards", 1)
                    .build()
            ).setMapping("""
                {
                  "properties" : {
                    "location": { "type" : "shape" },
                    "sort_key": { "type" : "long" }
                  }
                }
                """)
        );

        List<IndexRequestBuilder> docs = new ArrayList<>(NUM_DOCS);
        for (int i = 0; i < NUM_DOCS; i++) {
            docs.add(prepareIndex(index).setId(Integer.toString(i)).setSource("location", polygon(i), "sort_key", scrambled(i)));
        }
        // indexRandom asserts that no document failed to index; a mapping rejection would otherwise leave an empty index on which
        // every query below trivially passes.
        indexRandom(true, false, docs);
        assertHitCount(prepareSearch(index).setSize(0), NUM_DOCS);

        // Asserts no shard failed and that each shard really ended up with a single segment.
        forceMerge(true);
    }

    /** Uncorrelated with the document id, so sorting on it genuinely shuffles documents across pages. */
    private static long scrambled(int i) {
        return (i * 2654435761L) % 1000003L;
    }

    /** A regular {@link #VERTICES}-gon whose centre is unique per document, so no two documents share a geometry. */
    private static String polygon(int i) {
        double cx = (i % 100) + (i / 100) * 0.001;
        double cy = i % 50;
        double r = 0.4;
        StringBuilder sb = new StringBuilder("POLYGON((");
        for (int v = 0; v <= VERTICES; v++) {
            double angle = 2 * Math.PI * (v % VERTICES) / VERTICES;
            if (v > 0) {
                sb.append(", ");
            }
            sb.append(cx + r * Math.cos(angle)).append(' ').append(cy + r * Math.sin(angle));
        }
        return sb.append("))").toString();
    }
}
