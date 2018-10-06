/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.ml;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction.Response.JobStats;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.stats.ForecastStats;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.BaseMonitoringDocTestCase;
import org.joda.time.DateTime;
import org.junit.Before;

import java.io.IOException;
import java.util.Date;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link JobStatsMonitoringDocTests}.
 */
public class JobStatsMonitoringDocTests extends BaseMonitoringDocTestCase<JobStatsMonitoringDoc> {

    private JobStats jobStats;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        jobStats = mock(JobStats.class);
    }

    @Override
    protected JobStatsMonitoringDoc createMonitoringDoc(final String cluster, final long timestamp, long interval,
                                                        final MonitoringDoc.Node node, final MonitoredSystem system,
                                                        final String type, final String id) {
        return new JobStatsMonitoringDoc(cluster, timestamp, interval, node, jobStats);
    }

    @Override
    protected void assertMonitoringDoc(final JobStatsMonitoringDoc document) {
        assertThat(document.getSystem(), is(MonitoredSystem.ES));
        assertThat(document.getType(), is(JobStatsMonitoringDoc.TYPE));
        assertThat(document.getId(), nullValue());

        assertThat(document.getJobStats(), is(jobStats));
    }

    public void testConstructorJobStatsMustNotBeNull() {
        expectThrows(NullPointerException.class,
                     () -> new JobStatsMonitoringDoc(cluster, timestamp, interval, node, null));
    }

    @Override
    public void testToXContent() throws IOException {
        final TimeValue time = TimeValue.timeValueHours(13L);
        final Date date1 = DateTime.parse("2017-01-01T01:01:01.001+01").toDate();
        final Date date2 = DateTime.parse("2017-01-02T02:02:02.002+02").toDate();
        final Date date3 = DateTime.parse("2017-01-03T03:03:03.003+03").toDate();
        final Date date4 = DateTime.parse("2017-01-04T04:04:04.004+04").toDate();
        final Date date5 = DateTime.parse("2017-01-05T05:05:05.005+05").toDate();
        final Date date6 = DateTime.parse("2017-01-06T06:06:06.006+06").toDate();
        final Date date7 = DateTime.parse("2017-01-07T07:07:07.007+07").toDate();


        final DiscoveryNode discoveryNode = new DiscoveryNode("_node_name",
                                                             "_node_id",
                                                             "_ephemeral_id",
                                                             "_host_name",
                                                             "_host_address",
                                                             new TransportAddress(TransportAddress.META_ADDRESS, 9300),
                                                             singletonMap("attr", "value"),
                                                             singleton(DiscoveryNode.Role.MASTER),
                                                             Version.V_6_0_0_beta1);

        final ModelSizeStats modelStats = new ModelSizeStats.Builder("_model")
                                                            .setModelBytes(100L)
                                                            .setTotalByFieldCount(101L)
                                                            .setTotalOverFieldCount(102L)
                                                            .setTotalPartitionFieldCount(103L)
                                                            .setBucketAllocationFailuresCount(104L)
                                                            .setMemoryStatus(ModelSizeStats.MemoryStatus.OK)
                                                            .setTimestamp(date1)
                                                            .setLogTime(date2)
                                                            .build();

        final DataCounts dataCounts = new DataCounts("_job_id", 0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, date3, date4, date5, date6, date7);
        final ForecastStats forecastStats = new ForecastStats();
        final JobStats jobStats = new JobStats("_job", dataCounts, modelStats, forecastStats, JobState.OPENED, discoveryNode,
                "_explanation", time);
        final MonitoringDoc.Node node = new MonitoringDoc.Node("_uuid", "_host", "_addr", "_ip", "_name", 1504169190855L);

        final JobStatsMonitoringDoc document = new JobStatsMonitoringDoc("_cluster", 1502266739402L, 1506593717631L, node, jobStats);

        final BytesReference xContent = XContentHelper.toXContent(document, XContentType.JSON, false);
        assertEquals("{"
                     + "\"cluster_uuid\":\"_cluster\","
                     + "\"timestamp\":\"2017-08-09T08:18:59.402Z\","
                     + "\"interval_ms\":1506593717631,"
                     + "\"type\":\"job_stats\","
                     + "\"source_node\":{"
                       + "\"uuid\":\"_uuid\","
                       + "\"host\":\"_host\","
                       + "\"transport_address\":\"_addr\","
                       + "\"ip\":\"_ip\","
                       + "\"name\":\"_name\","
                       + "\"timestamp\":\"2017-08-31T08:46:30.855Z\""
                      + "},"
                     + "\"job_stats\":{"
                       + "\"job_id\":\"_job\","
                       + "\"data_counts\":{"
                         + "\"job_id\":\"_job_id\","
                         + "\"processed_record_count\":0,"
                         + "\"processed_field_count\":1,"
                         + "\"input_bytes\":2,"
                         + "\"input_field_count\":3,"
                         + "\"invalid_date_count\":4,"
                         + "\"missing_field_count\":5,"
                         + "\"out_of_order_timestamp_count\":6,"
                         + "\"empty_bucket_count\":7,"
                         + "\"sparse_bucket_count\":8,"
                         + "\"bucket_count\":9,"
                         + "\"earliest_record_timestamp\":1483401783003,"
                         + "\"latest_record_timestamp\":1483488244004,"
                         + "\"last_data_time\":1483574705005,"
                         + "\"latest_empty_bucket_timestamp\":1483661166006,"
                         + "\"latest_sparse_bucket_timestamp\":1483747627007,"
                         + "\"input_record_count\":10"
                        + "},"
                       + "\"model_size_stats\":{"
                         + "\"job_id\":\"_model\","
                         + "\"result_type\":\"model_size_stats\","
                         + "\"model_bytes\":100,"
                         + "\"total_by_field_count\":101,"
                         + "\"total_over_field_count\":102,"
                         + "\"total_partition_field_count\":103,"
                         + "\"bucket_allocation_failures_count\":104,"
                         + "\"memory_status\":\"ok\","
                         + "\"log_time\":1483315322002,"
                         + "\"timestamp\":1483228861001"
                        + "},"
                        + "\"forecasts_stats\":{"
                         + "\"total\":0,\"forecasted_jobs\":0"
                        + "},"
                       + "\"state\":\"opened\","
                       + "\"node\":{"
                         + "\"id\":\"_node_id\","
                         + "\"name\":\"_node_name\","
                         + "\"ephemeral_id\":\"_ephemeral_id\","
                         + "\"transport_address\":\"0.0.0.0:9300\","
                         + "\"attributes\":{"
                           + "\"attr\":\"value\""
                          + "}"
                        + "},"
                       + "\"assignment_explanation\":\"_explanation\","
                       + "\"open_time\":\"13h\""
                      + "}"
                    + "}", xContent.utf8ToString());
    }
}
