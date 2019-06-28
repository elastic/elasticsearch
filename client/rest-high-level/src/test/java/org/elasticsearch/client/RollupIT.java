/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.core.IndexerState;
import org.elasticsearch.client.rollup.DeleteRollupJobRequest;
import org.elasticsearch.client.rollup.GetRollupCapsRequest;
import org.elasticsearch.client.rollup.GetRollupCapsResponse;
import org.elasticsearch.client.rollup.GetRollupIndexCapsRequest;
import org.elasticsearch.client.rollup.GetRollupIndexCapsResponse;
import org.elasticsearch.client.rollup.GetRollupJobRequest;
import org.elasticsearch.client.rollup.GetRollupJobResponse;
import org.elasticsearch.client.rollup.GetRollupJobResponse.JobWrapper;
import org.elasticsearch.client.rollup.PutRollupJobRequest;
import org.elasticsearch.client.rollup.RollableIndexCaps;
import org.elasticsearch.client.rollup.RollupJobCaps;
import org.elasticsearch.client.rollup.StartRollupJobRequest;
import org.elasticsearch.client.rollup.StartRollupJobResponse;
import org.elasticsearch.client.rollup.StopRollupJobRequest;
import org.elasticsearch.client.rollup.StopRollupJobResponse;
import org.elasticsearch.client.rollup.job.config.DateHistogramGroupConfig;
import org.elasticsearch.client.rollup.job.config.GroupConfig;
import org.elasticsearch.client.rollup.job.config.MetricConfig;
import org.elasticsearch.client.rollup.job.config.RollupJobConfig;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

public class RollupIT extends ESRestHighLevelClientTestCase {

    double sum = 0.0d;
    int max = Integer.MIN_VALUE;
    int min = Integer.MAX_VALUE;
    private static final List<String> SUPPORTED_METRICS = Arrays.asList(MaxAggregationBuilder.NAME, MinAggregationBuilder.NAME,
        SumAggregationBuilder.NAME, AvgAggregationBuilder.NAME, ValueCountAggregationBuilder.NAME);

    private String id;
    private String indexPattern;
    private String rollupIndex;
    private String cron;
    private int pageSize;
    private int numDocs;

    @Before
    public void init() throws Exception {
        id = randomAlphaOfLength(10);
        indexPattern = randomFrom("docs", "d*", "doc*");
        rollupIndex = randomFrom("rollup", "test");
        cron = "*/1 * * * * ?";
        numDocs = indexDocs();
        pageSize = randomIntBetween(numDocs, numDocs * 10);
    }

    public int indexDocs() throws Exception {
        final BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int minute = 0; minute < 60; minute++) {
            for (int second = 0; second < 60; second = second + 10) {
                final int value = randomIntBetween(0, 100);

                final IndexRequest indexRequest = new IndexRequest("docs");
                indexRequest.source(jsonBuilder()
                    .startObject()
                    .field("value", value)
                    .field("date", String.format(Locale.ROOT, "2018-01-01T00:%02d:%02dZ", minute, second))
                    .endObject());
                bulkRequest.add(indexRequest);

                sum += value;
                if (value > max) {
                    max = value;
                }
                if (value < min) {
                    min = value;
                }
            }
        }

        final int numDocs = bulkRequest.numberOfActions();

        BulkResponse bulkResponse = highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);
        assertEquals(RestStatus.OK, bulkResponse.status());
        if (bulkResponse.hasFailures()) {
            for (BulkItemResponse itemResponse : bulkResponse.getItems()) {
                if (itemResponse.isFailed()) {
                    logger.fatal(itemResponse.getFailureMessage());
                }
            }
        }
        assertFalse(bulkResponse.hasFailures());

        RefreshResponse refreshResponse = highLevelClient().indices().refresh(new RefreshRequest("docs"), RequestOptions.DEFAULT);
        assertEquals(0, refreshResponse.getFailedShards());
        return numDocs;
    }


    public void testDeleteRollupJob() throws Exception {
        final GroupConfig groups = new GroupConfig(new DateHistogramGroupConfig.CalendarInterval("date", DateHistogramInterval.DAY));
        final List<MetricConfig> metrics = Collections.singletonList(new MetricConfig("value", SUPPORTED_METRICS));
        final TimeValue timeout = TimeValue.timeValueSeconds(randomIntBetween(30, 600));
        PutRollupJobRequest putRollupJobRequest =
            new PutRollupJobRequest(new RollupJobConfig(id, indexPattern, rollupIndex, cron, pageSize, groups, metrics, timeout));
        final RollupClient rollupClient = highLevelClient().rollup();
        execute(putRollupJobRequest, rollupClient::putRollupJob, rollupClient::putRollupJobAsync);
        DeleteRollupJobRequest deleteRollupJobRequest = new DeleteRollupJobRequest(id);
        AcknowledgedResponse deleteRollupJobResponse = highLevelClient().rollup()
            .deleteRollupJob(deleteRollupJobRequest, RequestOptions.DEFAULT);
        assertTrue(deleteRollupJobResponse.isAcknowledged());
    }

    public void testDeleteMissingRollupJob() {
        DeleteRollupJobRequest deleteRollupJobRequest = new DeleteRollupJobRequest(randomAlphaOfLength(10));
        ElasticsearchStatusException responseException = expectThrows(ElasticsearchStatusException.class,() -> highLevelClient().rollup()
            .deleteRollupJob(deleteRollupJobRequest, RequestOptions.DEFAULT));
        assertThat(responseException.status().getStatus(), is(404));
    }

    public void testPutStartAndGetRollupJob() throws Exception {
        // TODO expand this to also test with histogram and terms?
        final GroupConfig groups = new GroupConfig(new DateHistogramGroupConfig.CalendarInterval("date", DateHistogramInterval.DAY));
        final List<MetricConfig> metrics = Collections.singletonList(new MetricConfig("value", SUPPORTED_METRICS));
        final TimeValue timeout = TimeValue.timeValueSeconds(randomIntBetween(30, 600));

        PutRollupJobRequest putRollupJobRequest =
            new PutRollupJobRequest(new RollupJobConfig(id, indexPattern, rollupIndex, cron, pageSize, groups, metrics, timeout));

        final RollupClient rollupClient = highLevelClient().rollup();
        AcknowledgedResponse response = execute(putRollupJobRequest, rollupClient::putRollupJob, rollupClient::putRollupJobAsync);
        assertTrue(response.isAcknowledged());

        StartRollupJobRequest startRequest = new StartRollupJobRequest(id);
        StartRollupJobResponse startResponse = execute(startRequest, rollupClient::startRollupJob, rollupClient::startRollupJobAsync);
        assertTrue(startResponse.isAcknowledged());

        assertBusy(() -> {
            SearchResponse searchResponse = highLevelClient().search(new SearchRequest(rollupIndex), RequestOptions.DEFAULT);
            assertEquals(0, searchResponse.getFailedShards());
            assertEquals(1L, searchResponse.getHits().getTotalHits().value);

            SearchHit searchHit = searchResponse.getHits().getAt(0);
            Map<String, Object> source = searchHit.getSourceAsMap();
            assertNotNull(source);

            assertEquals(numDocs, source.get("date.date_histogram._count"));
            assertEquals(groups.getDateHistogram().getInterval().toString(), source.get("date.date_histogram.interval"));
            assertEquals(groups.getDateHistogram().getTimeZone(), source.get("date.date_histogram.time_zone"));

            for (MetricConfig metric : metrics) {
                for (String name : metric.getMetrics()) {
                    Number value = (Number) source.get(metric.getField() + "." + name + ".value");
                    if ("min".equals(name)) {
                        assertEquals(min, value.intValue());
                    } else if ("max".equals(name)) {
                        assertEquals(max, value.intValue());
                    } else if ("sum".equals(name)) {
                        assertEquals(sum, value.doubleValue(), 0.0d);
                    } else if ("avg".equals(name)) {
                        assertEquals(sum, value.doubleValue(), 0.0d);
                        Number avgCount = (Number) source.get(metric.getField() + "." + name + "._count");
                        assertEquals(numDocs, avgCount.intValue());
                    } else if ("value_count".equals(name)) {
                        assertEquals(numDocs, value.intValue());
                    }
                }
            }
        });

        GetRollupJobRequest getRollupJobRequest = randomBoolean() ? new GetRollupJobRequest() : new GetRollupJobRequest(id);
        GetRollupJobResponse getResponse = execute(getRollupJobRequest, rollupClient::getRollupJob, rollupClient::getRollupJobAsync);
        assertThat(getResponse.getJobs(), hasSize(1));
        JobWrapper job = getResponse.getJobs().get(0);
        assertEquals(putRollupJobRequest.getConfig(), job.getJob());
        assertThat(job.getStats().getNumPages(), lessThan(10L));
        assertEquals(numDocs, job.getStats().getNumDocuments());
        assertThat(job.getStats().getNumInvocations(), greaterThan(0L));
        assertEquals(1, job.getStats().getOutputDocuments());
        assertThat(job.getStatus().getState(), either(equalTo(IndexerState.STARTED)).or(equalTo(IndexerState.INDEXING)));
        assertThat(job.getStatus().getCurrentPosition(), hasKey("date.date_histogram"));

        // stop the job
        StopRollupJobRequest stopRequest = new StopRollupJobRequest(id);
        stopRequest.waitForCompletion(randomBoolean());
        StopRollupJobResponse stopResponse = execute(stopRequest, rollupClient::stopRollupJob, rollupClient::stopRollupJobAsync);
        assertTrue(stopResponse.isAcknowledged());
        if (stopRequest.waitForCompletion()) {
            getResponse = execute(new GetRollupJobRequest(id), rollupClient::getRollupJob, rollupClient::getRollupJobAsync);
            assertThat(getResponse.getJobs(), hasSize(1));
            assertThat(getResponse.getJobs().get(0).getStatus().getState(), equalTo(IndexerState.STOPPED));
        }
    }

    public void testSearch() throws Exception {
        testPutStartAndGetRollupJob();
        SearchRequest search = new SearchRequest(rollupIndex);
        search.source(new SearchSourceBuilder()
                .size(0)
                .aggregation(new AvgAggregationBuilder("avg").field("value")));
        SearchResponse response = highLevelClient().rollup().search(search, RequestOptions.DEFAULT);
        assertEquals(0, response.getFailedShards());
        assertEquals(0, response.getHits().getTotalHits().value);
        NumericMetricsAggregation.SingleValue avg = response.getAggregations().get("avg");
        assertThat(avg.value(), closeTo(sum / numDocs, 0.00000001));
    }

    public void testGetMissingRollupJob() throws Exception {
        GetRollupJobRequest getRollupJobRequest = new GetRollupJobRequest("missing");
        RollupClient rollupClient = highLevelClient().rollup();
        GetRollupJobResponse getResponse = execute(getRollupJobRequest, rollupClient::getRollupJob, rollupClient::getRollupJobAsync);
        assertThat(getResponse.getJobs(), empty());
    }

    public void testGetRollupCaps() throws Exception {
        final Set<Integer> values = new HashSet<>();
        double sum = 0.0d;
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;

        final BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int minute = 0; minute < 60; minute++) {
            for (int second = 0; second < 60; second = second + 10) {
                final int value = randomIntBetween(0, 100);

                final IndexRequest indexRequest = new IndexRequest("docs");
                indexRequest.source(jsonBuilder()
                    .startObject()
                    .field("value", value)
                    .field("date", String.format(Locale.ROOT, "2018-01-01T00:%02d:%02dZ", minute, second))
                    .endObject());
                bulkRequest.add(indexRequest);

                values.add(value);
                sum += value;
                if (value > max) {
                    max = value;
                }
                if (value < min) {
                    min = value;
                }
            }
        }

        final int numDocs = bulkRequest.numberOfActions();

        BulkResponse bulkResponse = highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);
        assertEquals(RestStatus.OK, bulkResponse.status());
        if (bulkResponse.hasFailures())     {
            for (BulkItemResponse itemResponse : bulkResponse.getItems()) {
                if (itemResponse.isFailed()) {
                    logger.fatal(itemResponse.getFailureMessage());
                }
            }
        }
        assertFalse(bulkResponse.hasFailures());

        RefreshResponse refreshResponse = highLevelClient().indices().refresh(new RefreshRequest("docs"), RequestOptions.DEFAULT);
        assertEquals(0, refreshResponse.getFailedShards());

        final String id = randomAlphaOfLength(10);
        final String indexPattern = randomFrom("docs", "d*", "doc*");
        final String rollupIndex = randomFrom("rollup", "test");
        final String cron = "*/1 * * * * ?";
        final int pageSize = randomIntBetween(numDocs, numDocs * 10);
        // TODO expand this to also test with histogram and terms?
        final GroupConfig groups = new GroupConfig(new DateHistogramGroupConfig.CalendarInterval("date", DateHistogramInterval.DAY));
        final List<MetricConfig> metrics = Collections.singletonList(new MetricConfig("value", SUPPORTED_METRICS));
        final TimeValue timeout = TimeValue.timeValueSeconds(randomIntBetween(30, 600));

        PutRollupJobRequest putRollupJobRequest =
            new PutRollupJobRequest(new RollupJobConfig(id, indexPattern, rollupIndex, cron, pageSize, groups, metrics, timeout));

        final RollupClient rollupClient = highLevelClient().rollup();
        AcknowledgedResponse response = execute(putRollupJobRequest, rollupClient::putRollupJob, rollupClient::putRollupJobAsync);
        assertTrue(response.isAcknowledged());

        // wait for the PutJob api to create the index w/ metadata
        highLevelClient().cluster().health(new ClusterHealthRequest(rollupIndex).waitForYellowStatus(), RequestOptions.DEFAULT);

        GetRollupCapsRequest getRollupCapsRequest = new GetRollupCapsRequest(indexPattern);
        GetRollupCapsResponse capsResponse = highLevelClient().rollup()
            .getRollupCapabilities(getRollupCapsRequest, RequestOptions.DEFAULT);

        assertNotNull(capsResponse);
        Map<String, RollableIndexCaps> rolledPatterns = capsResponse.getJobs();
        assertThat(rolledPatterns.size(), equalTo(1));

        RollableIndexCaps docsPattern = rolledPatterns.get(indexPattern);
        assertThat(docsPattern.getIndexName(), equalTo(indexPattern));

        List<RollupJobCaps> rollupJobs = docsPattern.getJobCaps();
        assertThat(rollupJobs.size(), equalTo(1));

        RollupJobCaps jobCaps = rollupJobs.get(0);
        assertThat(jobCaps.getJobID(), equalTo(id));
        assertThat(jobCaps.getRollupIndex(), equalTo(rollupIndex));
        assertThat(jobCaps.getIndexPattern(), equalTo(indexPattern));

        Map<String, RollupJobCaps.RollupFieldCaps> fieldCaps = jobCaps.getFieldCaps();

        List<Map<String, Object>> timestampCaps = fieldCaps.get("date").getAggs();
        for (Map.Entry<String, Object> entry : timestampCaps.get(0).entrySet()) {
            switch (entry.getKey()) {
                case "agg":
                    assertThat(entry.getValue(), equalTo("date_histogram"));
                    break;
                case "delay":
                    assertThat(entry.getValue(), equalTo("foo"));
                    break;
                case "calendar_interval":
                    assertThat(entry.getValue(), equalTo("1d"));
                    break;
                case "time_zone":
                    assertThat(entry.getValue(), equalTo("UTC"));
                    break;
                default:
                    fail("Unknown field cap: [" + entry.getKey() + "]");
            }
        }

        List<Map<String, Object>> valueCaps = fieldCaps.get("value").getAggs();
        assertThat(valueCaps.size(), equalTo(SUPPORTED_METRICS.size()));
    }

    public void testGetRollupIndexCaps() throws Exception {
        final Set<Integer> values = new HashSet<>();
        double sum = 0.0d;
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;

        final BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int minute = 0; minute < 60; minute++) {
            for (int second = 0; second < 60; second = second + 10) {
                final int value = randomIntBetween(0, 100);

                final IndexRequest indexRequest = new IndexRequest("docs");
                indexRequest.source(jsonBuilder()
                    .startObject()
                    .field("value", value)
                    .field("date", String.format(Locale.ROOT, "2018-01-01T00:%02d:%02dZ", minute, second))
                    .endObject());
                bulkRequest.add(indexRequest);

                values.add(value);
                sum += value;
                if (value > max) {
                    max = value;
                }
                if (value < min) {
                    min = value;
                }
            }
        }

        final int numDocs = bulkRequest.numberOfActions();

        BulkResponse bulkResponse = highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);
        assertEquals(RestStatus.OK, bulkResponse.status());
        if (bulkResponse.hasFailures())     {
            for (BulkItemResponse itemResponse : bulkResponse.getItems()) {
                if (itemResponse.isFailed()) {
                    logger.fatal(itemResponse.getFailureMessage());
                }
            }
        }
        assertFalse(bulkResponse.hasFailures());

        RefreshResponse refreshResponse = highLevelClient().indices().refresh(new RefreshRequest("docs"), RequestOptions.DEFAULT);
        assertEquals(0, refreshResponse.getFailedShards());

        final String id = randomAlphaOfLength(10);
        final String indexPattern = randomFrom("docs", "d*", "doc*");
        final String rollupIndex = randomFrom("rollup", "test");
        final String cron = "*/1 * * * * ?";
        final int pageSize = randomIntBetween(numDocs, numDocs * 10);
        // TODO expand this to also test with histogram and terms?
        final GroupConfig groups = new GroupConfig(new DateHistogramGroupConfig.CalendarInterval("date", DateHistogramInterval.DAY));
        final List<MetricConfig> metrics = Collections.singletonList(new MetricConfig("value", SUPPORTED_METRICS));
        final TimeValue timeout = TimeValue.timeValueSeconds(randomIntBetween(30, 600));

        PutRollupJobRequest putRollupJobRequest =
            new PutRollupJobRequest(new RollupJobConfig(id, indexPattern, rollupIndex, cron, pageSize, groups, metrics, timeout));

        final RollupClient rollupClient = highLevelClient().rollup();
        AcknowledgedResponse response = execute(putRollupJobRequest, rollupClient::putRollupJob, rollupClient::putRollupJobAsync);
        assertTrue(response.isAcknowledged());

        // wait for the PutJob api to create the index w/ metadata
        highLevelClient().cluster().health(new ClusterHealthRequest(rollupIndex).waitForYellowStatus(), RequestOptions.DEFAULT);

        GetRollupIndexCapsRequest getRollupIndexCapsRequest = new GetRollupIndexCapsRequest(rollupIndex);
        GetRollupIndexCapsResponse capsResponse = highLevelClient().rollup()
            .getRollupIndexCapabilities(getRollupIndexCapsRequest, RequestOptions.DEFAULT);

        assertNotNull(capsResponse);
        Map<String, RollableIndexCaps> rolledPatterns = capsResponse.getJobs();
        assertThat(rolledPatterns.size(), equalTo(1));

        RollableIndexCaps docsPattern = rolledPatterns.get(rollupIndex);
        assertThat(docsPattern.getIndexName(), equalTo(rollupIndex));

        List<RollupJobCaps> rollupJobs = docsPattern.getJobCaps();
        assertThat(rollupJobs.size(), equalTo(1));

        RollupJobCaps jobCaps = rollupJobs.get(0);
        assertThat(jobCaps.getJobID(), equalTo(id));
        assertThat(jobCaps.getRollupIndex(), equalTo(rollupIndex));
        assertThat(jobCaps.getIndexPattern(), equalTo(indexPattern));

        Map<String, RollupJobCaps.RollupFieldCaps> fieldCaps = jobCaps.getFieldCaps();

        List<Map<String, Object>> timestampCaps = fieldCaps.get("date").getAggs();
        for (Map.Entry<String, Object> entry : timestampCaps.get(0).entrySet()) {
            switch (entry.getKey()) {
                case "agg":
                    assertThat(entry.getValue(), equalTo("date_histogram"));
                    break;
                case "delay":
                    assertThat(entry.getValue(), equalTo("foo"));
                    break;
                case "calendar_interval":
                    assertThat(entry.getValue(), equalTo("1d"));
                    break;
                case "time_zone":
                    assertThat(entry.getValue(), equalTo("UTC"));
                    break;
                default:
                    fail("Unknown field cap: [" + entry.getKey() + "]");
            }
        }

        List<Map<String, Object>> valueCaps = fieldCaps.get("value").getAggs();
        assertThat(valueCaps.size(), equalTo(SUPPORTED_METRICS.size()));
    }
}
