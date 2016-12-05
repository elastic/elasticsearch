/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.prelert.job.results.Result;
import org.mockito.ArgumentCaptor;

import org.elasticsearch.xpack.prelert.job.results.AnomalyRecord;
import org.elasticsearch.xpack.prelert.job.results.Bucket;
import org.elasticsearch.xpack.prelert.job.results.BucketInfluencer;
import org.elasticsearch.xpack.prelert.job.results.Influencer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.mockito.Mockito.mock;


public class JobResultsPersisterTests extends ESTestCase {

    private static final String CLUSTER_NAME = "myCluster";
    private static final String JOB_ID = "foo";

    public void testPersistBucket_OneRecord() throws IOException {
        ArgumentCaptor<XContentBuilder> captor = ArgumentCaptor.forClass(XContentBuilder.class);
        BulkResponse response = mock(BulkResponse.class);
        String responseId = "abcXZY54321";
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME)
                .prepareIndex("prelertresults-" + JOB_ID, Result.TYPE.getPreferredName(), responseId, captor)
                .prepareIndex("prelertresults-" + JOB_ID, Result.TYPE.getPreferredName(), "", captor)
                .prepareBulk(response);

        Client client = clientBuilder.build();
        Bucket bucket = new Bucket("foo", new Date(), 123456);
        bucket.setAnomalyScore(99.9);
        bucket.setEventCount(57);
        bucket.setInitialAnomalyScore(88.8);
        bucket.setMaxNormalizedProbability(42.0);
        bucket.setProcessingTimeMs(8888);
        bucket.setRecordCount(1);

        BucketInfluencer bi = new BucketInfluencer(JOB_ID);
        bi.setAnomalyScore(14.15);
        bi.setInfluencerFieldName("biOne");
        bi.setInitialAnomalyScore(18.12);
        bi.setProbability(0.0054);
        bi.setRawAnomalyScore(19.19);
        bucket.addBucketInfluencer(bi);

        // We are adding a record but it shouldn't be persisted as part of the bucket
        AnomalyRecord record = new AnomalyRecord(JOB_ID);
        record.setAnomalyScore(99.8);
        bucket.setRecords(Arrays.asList(record));

        JobResultsPersister persister = new JobResultsPersister(Settings.EMPTY, client);
        persister.persistBucket(bucket);
        List<XContentBuilder> list = captor.getAllValues();
        assertEquals(2, list.size());

        String s = list.get(0).string();
        assertTrue(s.matches(".*anomaly_score.:99\\.9.*"));
        assertTrue(s.matches(".*initial_anomaly_score.:88\\.8.*"));
        assertTrue(s.matches(".*max_normalized_probability.:42\\.0.*"));
        assertTrue(s.matches(".*record_count.:1.*"));
        assertTrue(s.matches(".*event_count.:57.*"));
        assertTrue(s.matches(".*bucket_span.:123456.*"));
        assertTrue(s.matches(".*processing_time_ms.:8888.*"));

        s = list.get(1).string();
        assertTrue(s.matches(".*probability.:0\\.0054.*"));
        assertTrue(s.matches(".*influencer_field_name.:.biOne.*"));
        assertTrue(s.matches(".*initial_anomaly_score.:18\\.12.*"));
        assertTrue(s.matches(".*anomaly_score.:14\\.15.*"));
        assertTrue(s.matches(".*raw_anomaly_score.:19\\.19.*"));
    }

    public void testPersistRecords() throws IOException {
        ArgumentCaptor<XContentBuilder> captor = ArgumentCaptor.forClass(XContentBuilder.class);
        BulkResponse response = mock(BulkResponse.class);
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME)
                .prepareIndex("prelertresults-" + JOB_ID, Result.TYPE.getPreferredName(), "", captor)
                .prepareBulk(response);
        Client client = clientBuilder.build();

        List<AnomalyRecord> records = new ArrayList<>();
        AnomalyRecord r1 = new AnomalyRecord(JOB_ID);
        records.add(r1);
        List<Double> actuals = new ArrayList<>();
        actuals.add(5.0);
        actuals.add(5.1);
        r1.setActual(actuals);
        r1.setAnomalyScore(99.8);
        r1.setBucketSpan(42);
        r1.setByFieldName("byName");
        r1.setByFieldValue("byValue");
        r1.setCorrelatedByFieldValue("testCorrelations");
        r1.setDetectorIndex(3);
        r1.setFieldName("testFieldName");
        r1.setFunction("testFunction");
        r1.setFunctionDescription("testDescription");
        r1.setInitialNormalizedProbability(23.4);
        r1.setNormalizedProbability(0.005);
        r1.setOverFieldName("overName");
        r1.setOverFieldValue("overValue");
        r1.setPartitionFieldName("partName");
        r1.setPartitionFieldValue("partValue");
        r1.setProbability(0.1);
        List<Double> typicals = new ArrayList<>();
        typicals.add(0.44);
        typicals.add(998765.3);
        r1.setTypical(typicals);

        JobResultsPersister persister = new JobResultsPersister(Settings.EMPTY, client);
        persister.persistRecords(records);
        List<XContentBuilder> captured = captor.getAllValues();
        assertEquals(1, captured.size());

        String s = captured.get(0).string();
        assertTrue(s.matches(".*detector_index.:3.*"));
        assertTrue(s.matches(".*\"probability\":0\\.1.*"));
        assertTrue(s.matches(".*\"anomaly_score\":99\\.8.*"));
        assertTrue(s.matches(".*\"normalized_probability\":0\\.005.*"));
        assertTrue(s.matches(".*initial_normalized_probability.:23.4.*"));
        assertTrue(s.matches(".*bucket_span.:42.*"));
        assertTrue(s.matches(".*by_field_name.:.byName.*"));
        assertTrue(s.matches(".*by_field_value.:.byValue.*"));
        assertTrue(s.matches(".*correlated_by_field_value.:.testCorrelations.*"));
        assertTrue(s.matches(".*typical.:.0\\.44,998765\\.3.*"));
        assertTrue(s.matches(".*actual.:.5\\.0,5\\.1.*"));
        assertTrue(s.matches(".*field_name.:.testFieldName.*"));
        assertTrue(s.matches(".*function.:.testFunction.*"));
        assertTrue(s.matches(".*function_description.:.testDescription.*"));
        assertTrue(s.matches(".*partition_field_name.:.partName.*"));
        assertTrue(s.matches(".*partition_field_value.:.partValue.*"));
        assertTrue(s.matches(".*over_field_name.:.overName.*"));
        assertTrue(s.matches(".*over_field_value.:.overValue.*"));
    }

    public void testPersistInfluencers() throws IOException {
        ArgumentCaptor<XContentBuilder> captor = ArgumentCaptor.forClass(XContentBuilder.class);
        BulkResponse response = mock(BulkResponse.class);
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME)
                .prepareIndex("prelertresults-" + JOB_ID, Result.TYPE.getPreferredName(), "", captor)
                .prepareBulk(response);
        Client client = clientBuilder.build();

        List<Influencer> influencers = new ArrayList<>();
        Influencer inf = new Influencer(JOB_ID, "infName1", "infValue1");
        inf.setAnomalyScore(16);
        inf.setId("infID");
        inf.setInitialAnomalyScore(55.5);
        inf.setProbability(0.4);
        influencers.add(inf);

        JobResultsPersister persister = new JobResultsPersister(Settings.EMPTY, client);
        persister.persistInfluencers(influencers);
        List<XContentBuilder> captured = captor.getAllValues();
        assertEquals(1, captured.size());

        String s = captured.get(0).string();
        assertTrue(s.matches(".*probability.:0\\.4.*"));
        assertTrue(s.matches(".*influencer_field_name.:.infName1.*"));
        assertTrue(s.matches(".*influencer_field_value.:.infValue1.*"));
        assertTrue(s.matches(".*initial_anomaly_score.:55\\.5.*"));
        assertTrue(s.matches(".*anomaly_score.:16\\.0.*"));
    }
}
