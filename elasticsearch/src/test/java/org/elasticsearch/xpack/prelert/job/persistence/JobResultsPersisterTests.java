/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;

import org.elasticsearch.xpack.prelert.job.results.AnomalyRecord;
import org.elasticsearch.xpack.prelert.job.results.Bucket;
import org.elasticsearch.xpack.prelert.job.results.BucketInfluencer;
import org.elasticsearch.xpack.prelert.job.results.Influencer;


public class JobResultsPersisterTests extends ESTestCase {

    private static final String CLUSTER_NAME = "myCluster";
    private static final String JOB_ID = "foo";

    public void testPersistBucket_NoRecords() {
        Client client = mock(Client.class);
        Bucket bucket = mock(Bucket.class);
        when(bucket.getRecords()).thenReturn(null);
        JobResultsPersister persister = new JobResultsPersister(Settings.EMPTY, client);
        persister.persistBucket(bucket);
        verifyNoMoreInteractions(client);
    }

    public void testPersistBucket_OneRecord() throws IOException {
        ArgumentCaptor<XContentBuilder> captor = ArgumentCaptor.forClass(XContentBuilder.class);
        BulkResponse response = mock(BulkResponse.class);
        String responseId = "abcXZY54321";
        MockClientBuilder clientBuilder = new MockClientBuilder(CLUSTER_NAME)
                .prepareIndex("prelertresults-" + JOB_ID, Bucket.TYPE.getPreferredName(), responseId, captor)
                .prepareIndex("prelertresults-" + JOB_ID, AnomalyRecord.TYPE.getPreferredName(), "", captor)
                .prepareIndex("prelertresults-" + JOB_ID, BucketInfluencer.TYPE.getPreferredName(), "", captor)
                .prepareIndex("prelertresults-" + JOB_ID, Influencer.TYPE.getPreferredName(), "", captor)
                .prepareBulk(response);

        Client client = clientBuilder.build();
        Bucket bucket = getBucket(1);
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

        Influencer inf = new Influencer("jobname", "infName1", "infValue1");
        inf.setAnomalyScore(16);
        inf.setId("infID");
        inf.setInitialAnomalyScore(55.5);
        inf.setProbability(0.4);
        inf.setTimestamp(bucket.getTimestamp());
        bucket.setInfluencers(Collections.singletonList(inf));

        AnomalyRecord record = bucket.getRecords().get(0);
        List<Double> actuals = new ArrayList<>();
        actuals.add(5.0);
        actuals.add(5.1);
        record.setActual(actuals);
        record.setAnomalyScore(99.8);
        record.setBucketSpan(42);
        record.setByFieldName("byName");
        record.setByFieldValue("byValue");
        record.setCorrelatedByFieldValue("testCorrelations");
        record.setDetectorIndex(3);
        record.setFieldName("testFieldName");
        record.setFunction("testFunction");
        record.setFunctionDescription("testDescription");
        record.setInitialNormalizedProbability(23.4);
        record.setNormalizedProbability(0.005);
        record.setOverFieldName("overName");
        record.setOverFieldValue("overValue");
        record.setPartitionFieldName("partName");
        record.setPartitionFieldValue("partValue");
        record.setProbability(0.1);
        List<Double> typicals = new ArrayList<>();
        typicals.add(0.44);
        typicals.add(998765.3);
        record.setTypical(typicals);

        JobResultsPersister persister = new JobResultsPersister(Settings.EMPTY, client);
        persister.persistBucket(bucket);
        List<XContentBuilder> list = captor.getAllValues();
        assertEquals(4, list.size());

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

        s = list.get(2).string();
        assertTrue(s.matches(".*probability.:0\\.4.*"));
        assertTrue(s.matches(".*influencerFieldName.:.infName1.*"));
        assertTrue(s.matches(".*influencerFieldValue.:.infValue1.*"));
        assertTrue(s.matches(".*initialAnomalyScore.:55\\.5.*"));
        assertTrue(s.matches(".*anomalyScore.:16\\.0.*"));

        s = list.get(3).string();
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

    private Bucket getBucket(int numRecords) {
        Bucket b = new Bucket(JOB_ID);
        b.setId("1");
        b.setTimestamp(new Date());
        List<AnomalyRecord> records = new ArrayList<>();
        for (int i = 0; i < numRecords; ++i) {
            AnomalyRecord r = new AnomalyRecord("foo");
            records.add(r);
        }
        b.setRecords(records);
        return b;
    }


}
