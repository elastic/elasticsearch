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
import org.elasticsearch.xpack.prelert.job.results.AnomalyRecord;
import org.elasticsearch.xpack.prelert.job.results.Bucket;
import org.elasticsearch.xpack.prelert.job.results.BucketInfluencer;
import org.elasticsearch.xpack.prelert.job.results.Influencer;
import org.mockito.ArgumentCaptor;

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
                .prepareIndex("prelertresults-" + JOB_ID, Bucket.TYPE.getPreferredName(), responseId, captor)
                .prepareIndex("prelertresults-" + JOB_ID, BucketInfluencer.TYPE.getPreferredName(), "", captor)
                .prepareBulk(response);

        Client client = clientBuilder.build();
        Bucket bucket = new Bucket("foo");
        bucket.setId("1");
        bucket.setTimestamp(new Date());
        bucket.setId(responseId);
        bucket.setAnomalyScore(99.9);
        bucket.setBucketSpan(123456);
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
        assertTrue(s.matches(".*anomalyScore.:99\\.9.*"));
        assertTrue(s.matches(".*initialAnomalyScore.:88\\.8.*"));
        assertTrue(s.matches(".*maxNormalizedProbability.:42\\.0.*"));
        assertTrue(s.matches(".*recordCount.:1.*"));
        assertTrue(s.matches(".*eventCount.:57.*"));
        assertTrue(s.matches(".*bucketSpan.:123456.*"));
        assertTrue(s.matches(".*processingTimeMs.:8888.*"));

        s = list.get(1).string();
        assertTrue(s.matches(".*probability.:0\\.0054.*"));
        assertTrue(s.matches(".*influencerFieldName.:.biOne.*"));
        assertTrue(s.matches(".*initialAnomalyScore.:18\\.12.*"));
        assertTrue(s.matches(".*anomalyScore.:14\\.15.*"));
        assertTrue(s.matches(".*rawAnomalyScore.:19\\.19.*"));
    }

    public void testPersistRecords() throws IOException {
        ArgumentCaptor<XContentBuilder> captor = ArgumentCaptor.forClass(XContentBuilder.class);
        BulkResponse response = mock(BulkResponse.class);
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME)
                .prepareIndex("prelertresults-" + JOB_ID, AnomalyRecord.TYPE.getPreferredName(), "", captor)
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
        assertTrue(s.matches(".*detectorIndex.:3.*"));
        assertTrue(s.matches(".*\"probability\":0\\.1.*"));
        assertTrue(s.matches(".*\"anomalyScore\":99\\.8.*"));
        assertTrue(s.matches(".*\"normalizedProbability\":0\\.005.*"));
        assertTrue(s.matches(".*initialNormalizedProbability.:23.4.*"));
        assertTrue(s.matches(".*bucketSpan.:42.*"));
        assertTrue(s.matches(".*byFieldName.:.byName.*"));
        assertTrue(s.matches(".*byFieldValue.:.byValue.*"));
        assertTrue(s.matches(".*correlatedByFieldValue.:.testCorrelations.*"));
        assertTrue(s.matches(".*typical.:.0\\.44,998765\\.3.*"));
        assertTrue(s.matches(".*actual.:.5\\.0,5\\.1.*"));
        assertTrue(s.matches(".*fieldName.:.testFieldName.*"));
        assertTrue(s.matches(".*function.:.testFunction.*"));
        assertTrue(s.matches(".*functionDescription.:.testDescription.*"));
        assertTrue(s.matches(".*partitionFieldName.:.partName.*"));
        assertTrue(s.matches(".*partitionFieldValue.:.partValue.*"));
        assertTrue(s.matches(".*overFieldName.:.overName.*"));
        assertTrue(s.matches(".*overFieldValue.:.overValue.*"));
    }

    public void testPersistInfluencers() throws IOException {
        ArgumentCaptor<XContentBuilder> captor = ArgumentCaptor.forClass(XContentBuilder.class);
        BulkResponse response = mock(BulkResponse.class);
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME)
                .prepareIndex("prelertresults-" + JOB_ID, Influencer.TYPE.getPreferredName(), "", captor)
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
        assertTrue(s.matches(".*influencerFieldName.:.infName1.*"));
        assertTrue(s.matches(".*influencerFieldValue.:.infValue1.*"));
        assertTrue(s.matches(".*initialAnomalyScore.:55\\.5.*"));
        assertTrue(s.matches(".*anomalyScore.:16\\.0.*"));
    }
}
