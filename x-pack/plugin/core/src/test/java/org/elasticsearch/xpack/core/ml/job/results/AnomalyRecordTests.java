/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.results;

import org.elasticsearch.client.ml.job.config.DetectorFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class AnomalyRecordTests extends AbstractSerializingTestCase<AnomalyRecord> {

    @Override
    protected AnomalyRecord createTestInstance() {
        return createTestInstance("foo");
    }

    public AnomalyRecord createTestInstance(String jobId) {
        AnomalyRecord anomalyRecord = new AnomalyRecord(jobId, new Date(randomNonNegativeLong()), randomNonNegativeLong());
        anomalyRecord.setActual(Collections.singletonList(randomDouble()));
        anomalyRecord.setTypical(Collections.singletonList(randomDouble()));
        anomalyRecord.setProbability(randomDouble());
        if (randomBoolean()) {
            anomalyRecord.setMultiBucketImpact(randomDouble());
        }
        anomalyRecord.setRecordScore(randomDouble());
        anomalyRecord.setInitialRecordScore(randomDouble());
        anomalyRecord.setInterim(randomBoolean());
        if (randomBoolean()) {
            anomalyRecord.setFieldName(randomAlphaOfLength(12));
        }
        if (randomBoolean()) {
            anomalyRecord.setByFieldName(randomAlphaOfLength(12));
            anomalyRecord.setByFieldValue(randomAlphaOfLength(12));
        }
        if (randomBoolean()) {
            anomalyRecord.setPartitionFieldName(randomAlphaOfLength(12));
            anomalyRecord.setPartitionFieldValue(randomAlphaOfLength(12));
        }
        if (randomBoolean()) {
            anomalyRecord.setOverFieldName(randomAlphaOfLength(12));
            anomalyRecord.setOverFieldValue(randomAlphaOfLength(12));
        }
        if (randomBoolean()) {
            anomalyRecord.setFunction(DetectorFunction.LAT_LONG.getFullName());
            anomalyRecord.setGeoResults(GeoResultsTests.createTestGeoResults());
        } else {
            anomalyRecord.setFunction(randomAlphaOfLengthBetween(5, 25));
        }
        anomalyRecord.setFunctionDescription(randomAlphaOfLengthBetween(5, 20));
        if (randomBoolean()) {
            anomalyRecord.setCorrelatedByFieldValue(randomAlphaOfLength(16));
        }
        if (randomBoolean()) {
            int count = randomIntBetween(0, 9);
            List<Influence>  influences = new ArrayList<>();
            for (int i=0; i<count; i++) {
                influences.add(new Influence(randomAlphaOfLength(8), Collections.singletonList(randomAlphaOfLengthBetween(1, 28))));
            }
            anomalyRecord.setInfluencers(influences);
        }
        if (randomBoolean()) {
            int count = randomIntBetween(0, 9);
            List<AnomalyCause>  causes = new ArrayList<>();
            for (int i=0; i<count; i++) {
                causes.add(new AnomalyCauseTests().createTestInstance());
            }
            anomalyRecord.setCauses(causes);
        }

        return anomalyRecord;
    }

    @Override
    protected Writeable.Reader<AnomalyRecord> instanceReader() {
        return AnomalyRecord::new;
    }

    @Override
    protected AnomalyRecord doParseInstance(XContentParser parser) {
        return AnomalyRecord.STRICT_PARSER.apply(parser, null);
    }

    @SuppressWarnings("unchecked")
    public void testToXContentIncludesInputFields() throws IOException {
        AnomalyRecord record = createTestInstance();
        record.setByFieldName("byfn");
        record.setByFieldValue("byfv");
        record.setOverFieldName("overfn");
        record.setOverFieldValue("overfv");
        record.setPartitionFieldName("partfn");
        record.setPartitionFieldValue("partfv");

        Influence influence1 = new Influence("inffn", Arrays.asList("inffv1", "inffv2"));
        Influence influence2 = new Influence("inffn", Arrays.asList("inffv1", "inffv2"));
        record.setInfluencers(Arrays.asList(influence1, influence2));

        BytesReference bytes = XContentHelper.toXContent(record, XContentType.JSON, false);
        XContentParser parser = createParser(XContentType.JSON.xContent(), bytes);
        Map<String, Object> map = parser.map();
        List<String> serialisedByFieldValues = (List<String>) map.get(record.getByFieldName());
        assertEquals(Collections.singletonList(record.getByFieldValue()), serialisedByFieldValues);
        List<String> serialisedOverFieldValues = (List<String>) map.get(record.getOverFieldName());
        assertEquals(Collections.singletonList(record.getOverFieldValue()), serialisedOverFieldValues);
        List<String> serialisedPartFieldValues = (List<String>) map.get(record.getPartitionFieldName());
        assertEquals(Collections.singletonList(record.getPartitionFieldValue()), serialisedPartFieldValues);

        List<String> serialisedInfFieldValues1 = (List<String>) map.get(influence1.getInfluencerFieldName());
        assertEquals(influence1.getInfluencerFieldValues(), serialisedInfFieldValues1);
        List<String> serialisedInfFieldValues2 = (List<String>) map.get(influence2.getInfluencerFieldName());
        assertEquals(influence2.getInfluencerFieldValues(), serialisedInfFieldValues2);
    }

    public void testToXContentOrdersDuplicateInputFields() throws IOException {
        AnomalyRecord record = createTestInstance();
        record.setByFieldName("car-make");
        record.setByFieldValue("ford");
        record.setOverFieldName("number-of-wheels");
        record.setOverFieldValue("4");
        record.setPartitionFieldName("spoiler");
        record.setPartitionFieldValue("yes");

        Influence influence1 = new Influence("car-make", Collections.singletonList("VW"));
        Influence influence2 = new Influence("number-of-wheels", Collections.singletonList("18"));
        Influence influence3 = new Influence("spoiler", Collections.singletonList("no"));
        record.setInfluencers(Arrays.asList(influence1, influence2, influence3));

        // influencer fields with the same name as a by/over/partitiion field
        // come second in the list
        BytesReference bytes = XContentHelper.toXContent(record, XContentType.JSON, false);
        XContentParser parser = createParser(XContentType.JSON.xContent(), bytes);
        Map<String, Object> map = parser.map();
        List<String> serialisedCarMakeFieldValues = (List<String>) map.get("car-make");
        assertEquals(Arrays.asList("ford", "VW"), serialisedCarMakeFieldValues);
        List<String> serialisedNumberOfWheelsFieldValues = (List<String>) map.get("number-of-wheels");
        assertEquals(Arrays.asList("4", "18"), serialisedNumberOfWheelsFieldValues);
        List<String> serialisedSpoilerFieldValues = (List<String>) map.get("spoiler");
        assertEquals(Arrays.asList("yes", "no"), serialisedSpoilerFieldValues);
    }

    public void testToXContentDoesNotIncludesReservedWordInputFields() throws IOException {
        AnomalyRecord record = createTestInstance();
        record.setByFieldName(AnomalyRecord.BUCKET_SPAN.getPreferredName());
        record.setByFieldValue("bar");

        BytesReference bytes = XContentHelper.toXContent(record, XContentType.JSON, false);
        XContentParser parser = createParser(XContentType.JSON.xContent(), bytes);
        Object value = parser.map().get(AnomalyRecord.BUCKET_SPAN.getPreferredName());
        assertNotEquals("bar", value);
        assertEquals(record.getBucketSpan(), value);
    }

    public void testId() {
        AnomalyRecord record = new AnomalyRecord("test-job", new Date(1000), 60L);
        String byFieldValue = null;
        String overFieldValue = null;
        String partitionFieldValue = null;

        assertEquals("test-job_record_1000_60_0_0_0", record.getId());

        if (randomBoolean()) {
            byFieldValue = randomAlphaOfLength(10);
            record.setByFieldValue(byFieldValue);
        }
        if (randomBoolean()) {
            overFieldValue = randomAlphaOfLength(10);
            record.setOverFieldValue(overFieldValue);
        }
        if (randomBoolean()) {
            partitionFieldValue = randomAlphaOfLength(10);
            record.setPartitionFieldValue(partitionFieldValue);
        }

        String valuesPart = MachineLearningField.valuesToId(byFieldValue, overFieldValue, partitionFieldValue);
        assertEquals("test-job_record_1000_60_0_" + valuesPart, record.getId());
    }

    public void testStrictParser_IsLenientOnTopLevelFields() throws IOException {
        String json = "{\"job_id\":\"job_1\", \"timestamp\": 123544456, \"bucket_span\": 3600, \"foo\":\"bar\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            AnomalyRecord.STRICT_PARSER.apply(parser, null);
        }
    }

    public void testStrictParser_IsStrictOnNestedFields() throws IOException {
        String json = "{\"job_id\":\"job_1\", \"timestamp\": 123544456, \"bucket_span\": 3600, \"foo\":\"bar\"," +
                " \"causes\":[{\"cause_foo\":\"bar\"}]}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            XContentParseException e = expectThrows(XContentParseException.class,
                    () -> AnomalyRecord.STRICT_PARSER.apply(parser, null));
            assertThat(e.getCause().getMessage(), containsString("[anomaly_cause] unknown field [cause_foo]"));
        }
    }

    public void testLenientParser() throws IOException {
        String json = "{\"job_id\":\"job_1\", \"timestamp\": 123544456, \"bucket_span\": 3600, \"foo\":\"bar\"," +
                " \"causes\":[{\"cause_foo\":\"bar\"}]}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            AnomalyRecord.LENIENT_PARSER.apply(parser, null);
        }
    }

    public void testIdLength() {
        String jobId = randomAlphaOfLength(MlStrings.ID_LENGTH_LIMIT);
        Date timestamp = new Date(Long.MAX_VALUE);
        long bucketSpan = Long.MAX_VALUE;
        int detectorIndex = Integer.MAX_VALUE;
        String byFieldValue = randomAlphaOfLength(randomIntBetween(100, 1000));
        String overFieldValue = randomAlphaOfLength(randomIntBetween(100, 1000));
        String partitionFieldValue = randomAlphaOfLength(randomIntBetween(100, 1000));

        String id = AnomalyRecord.buildId(jobId, timestamp, bucketSpan, detectorIndex, byFieldValue, overFieldValue, partitionFieldValue);
        // 512 comes from IndexRequest.validate()
        assertThat(id.getBytes(StandardCharsets.UTF_8).length, lessThanOrEqualTo(512));
    }
}
