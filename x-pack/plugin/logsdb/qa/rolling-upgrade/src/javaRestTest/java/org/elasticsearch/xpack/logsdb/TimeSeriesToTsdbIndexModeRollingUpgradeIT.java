/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.client.Request;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.logsdb.TsdbIT.getTemplate;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Verifies that a data stream's backing indices can switch from {@link IndexMode#TIME_SERIES} to
 * its preferred alias {@link IndexMode#TSDB} via a template update + rollover, once the whole
 * cluster has been upgraded to a version that understands {@code tsdb} (old-version clusters in
 * this suite never do, since {@code tsdb} did not exist as an {@code index.mode} value before).
 * The pre-switch ({@code time_series}) and post-switch ({@code tsdb}) backing indices must both
 * remain queryable, together, once the switch has happened.
 */
public class TimeSeriesToTsdbIndexModeRollingUpgradeIT extends AbstractLogsdbRollingUpgradeTestCase {

    private static final String DATA_STREAM_NAME = "k10s";
    private static final int DOCS_PER_ROUND = 4 * 128;

    // Kept short (and explicit, rather than relying on the plugin's defaults) so the test can
    // deterministically place documents into the pre-switch vs. post-switch backing index by
    // choosing their @timestamp, rather than depending on how much real wall-clock time the
    // rolling upgrade itself takes. A backing index's [start_time, end_time) window is anchored
    // to its real creation time, not to any document's @timestamp; on rollover the new index's
    // start_time is exactly the old index's end_time (contiguous, no overlap/gap) - see
    // DataStreamIndexSettingsProvider#provideAdditionalSettings.
    private static final String LOOK_AHEAD_AND_BACK_TIME = "1m";

    private int totalDocsIndexed = 0;

    public void testSwitchFromTimeSeriesToTsdb() throws Exception {
        String templateId = getClass().getSimpleName().toLowerCase(Locale.ROOT);
        String timeSeriesTemplate = withLookAheadAndBack(getTemplate(null, null));
        createTemplate(DATA_STREAM_NAME, templateId, timeSeriesTemplate);

        // Anchor for the whole test: the first backing index's window is [testStart - 1m, testStart
        // + 1m). Documents timestamped near testStart land there regardless of real elapsed time.
        Instant testStart = Instant.now();

        // Old cluster: only "time_series" exists as an index.mode value.
        indexRound(testStart);
        verifyWriteIndexMode(IndexMode.TIME_SERIES);
        assertSearchAndQuery();

        // Mixed cluster: keep indexing/querying against the still-time_series write index.
        clusterRollingUpgrade(index -> {
            indexRound(testStart.plusSeconds(index + 1));
            verifyWriteIndexMode(IndexMode.TIME_SERIES);
            assertSearchAndQuery();
        });

        String firstBackingIndex = getDataStreamBackingIndexNames(DATA_STREAM_NAME).getFirst();

        // Fully upgraded: switch the template to "tsdb" and roll over onto a new backing index.
        String tsdbTemplate = withLookAheadAndBack(getTemplate(null, null)).replace("\"time_series\"", "\"tsdb\"");
        createTemplate(DATA_STREAM_NAME, templateId, tsdbTemplate);
        rolloverDataStream(DATA_STREAM_NAME);

        List<String> backingIndices = getDataStreamBackingIndexNames(DATA_STREAM_NAME);
        assertThat(backingIndices, hasSize(2));
        assertThat(backingIndices.get(0), equalTo(firstBackingIndex));
        String secondBackingIndex = backingIndices.get(1);

        verifyIndexMode(IndexMode.TIME_SERIES, firstBackingIndex);
        verifyIndexMode(IndexMode.TSDB, secondBackingIndex);

        // The second backing index's window starts exactly where the first one's ends
        // (testStart + 1m); land comfortably inside it, regardless of how long the upgrade took.
        indexRound(testStart.plusSeconds(90));
        assertSearchAndQuery();

        var forceMergeRequest = new Request("POST", "/" + DATA_STREAM_NAME + "/_forcemerge");
        forceMergeRequest.addParameter("max_num_segments", "1");
        assertOK(client().performRequest(forceMergeRequest));
        ensureGreen(DATA_STREAM_NAME);
        assertSearchAndQuery();
    }

    private static String withLookAheadAndBack(String template) {
        return template.replace(
            "\"mode\": \"time_series\"",
            "\"mode\": \"time_series\", \"look_ahead_time\": \""
                + LOOK_AHEAD_AND_BACK_TIME
                + "\", \"look_back_time\": \""
                + LOOK_AHEAD_AND_BACK_TIME
                + "\""
        );
    }

    private void indexRound(Instant timestamp) throws Exception {
        bulkIndex(DATA_STREAM_NAME, 4, 128, timestamp, TsdbIndexingRollingUpgradeIT::docSupplier);
        totalDocsIndexed += DOCS_PER_ROUND;
    }

    private void rolloverDataStream(String dataStreamName) throws IOException {
        var request = new Request("POST", "/" + dataStreamName + "/_rollover");
        assertOK(client().performRequest(request));
    }

    private void verifyWriteIndexMode(IndexMode indexMode) throws IOException {
        verifyIndexMode(indexMode, getDataStreamBackingIndexNames(DATA_STREAM_NAME).getLast());
    }

    private void verifyIndexMode(IndexMode indexMode, String index) throws IOException {
        var settings = (Map<?, ?>) getIndexSettings(index, true).get(index);
        assertThat(((Map<?, ?>) settings.get("settings")).get(IndexSettings.MODE.getKey()), equalTo(indexMode.getName()));
    }

    private void assertSearchAndQuery() throws Exception {
        search();
        query();
    }

    private void search() throws Exception {
        var searchRequest = new Request("POST", "/" + DATA_STREAM_NAME + "/_search");
        searchRequest.setJsonEntity("{\"size\": 0}");
        var response = client().performRequest(searchRequest);
        assertOK(response);
        var responseBody = entityAsMap(response);
        Integer totalCount = ObjectPath.evaluate(responseBody, "hits.total.value");
        assertThat(totalCount, equalTo(totalDocsIndexed));
    }

    /**
     * Groups by {@code _index} to prove the data stream query spans every backing index -
     * including both the {@code time_series} and the {@code tsdb} one once the switch has
     * happened - and that their per-index counts sum to every document ever indexed.
     */
    private void query() throws Exception {
        var queryRequest = new Request("POST", "/_query");
        queryRequest.setJsonEntity("""
            {
                "query": "FROM $ds METADATA _index | STATS count = COUNT(*) BY _index | SORT _index | LIMIT 10"
            }
            """.replace("$ds", DATA_STREAM_NAME));
        var response = client().performRequest(queryRequest);
        assertOK(response);
        var responseBody = entityAsMap(response);

        List<String> backingIndices = getDataStreamBackingIndexNames(DATA_STREAM_NAME);
        List<List<Object>> values = ObjectPath.evaluate(responseBody, "values");
        assertThat("values=" + values, values, hasSize(backingIndices.size()));

        long countedTotal = 0;
        List<String> queriedIndices = new ArrayList<>();
        for (List<Object> row : values) {
            Number count = (Number) row.get(0);
            assertThat(count, notNullValue());
            countedTotal += count.longValue();
            queriedIndices.add((String) row.get(1));
        }
        assertThat(countedTotal, equalTo((long) totalDocsIndexed));
        assertThat(queriedIndices, containsInAnyOrder(backingIndices.toArray()));
    }
}
