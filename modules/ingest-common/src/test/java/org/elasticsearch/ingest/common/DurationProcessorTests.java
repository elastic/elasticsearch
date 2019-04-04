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

package org.elasticsearch.ingest.common;

import java.util.concurrent.TimeUnit;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.hamcrest.CoreMatchers;

public class DurationProcessorTests extends AbstractStringProcessorTestCase<Long> {

    private String modifiedInput;

    @Override
    protected AbstractStringProcessor<Long> newProcessor(String field, boolean ignoreMissing, String targetField) {
        return new DurationProcessor(randomAlphaOfLength(10), field, ignoreMissing, targetField, TimeUnit.MILLISECONDS);
    }

    private AbstractStringProcessor<Long> newProcessor(String field, boolean ignoreMissing, String targetField, TimeUnit targetUnit) {
        return new DurationProcessor(randomAlphaOfLength(10), field, ignoreMissing, targetField, targetUnit);
    }

    @Override
    protected String modifyInput(String input) {
        // largest value that allows all results < Long.MAX_VALUE nanos
        long randomNumber = randomLongBetween(1, Long.MAX_VALUE / TimeUnit.DAYS.toNanos(1));
        TimeUnit randomUnit = randomFrom(TimeUnit.values());
        switch (randomUnit) {
            case NANOSECONDS:
                modifiedInput = randomNumber + "nanos";
                break;
            case MICROSECONDS:
                modifiedInput = randomNumber + "micros";
                break;
            case MILLISECONDS:
                modifiedInput = randomNumber + "ms";
                break;
            case SECONDS:
                modifiedInput = randomNumber + "s";
                break;
            case MINUTES:
                modifiedInput = randomNumber + "m";
                break;
            case HOURS:
                modifiedInput = randomNumber + "h";
                break;
            case DAYS:
                modifiedInput = randomNumber + "d";
                break;
        }
        return modifiedInput;
    }

    @Override
    protected Long expectedResult(String input) {
        return TimeValue.parseTimeValue(modifiedInput, null, "").getMillis();
    }

    @Override
    protected Class<Long> expectedResultType() {
        return Long.class;
    }

    public void testUnitConversion() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "7s");
        Processor processor = newProcessor(fieldName, randomBoolean(), fieldName, TimeUnit.NANOSECONDS);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, expectedResultType()), CoreMatchers.equalTo(7000000000L));
    }

    public void testTooLarge() {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "106752d");
        Processor processor = newProcessor(fieldName, randomBoolean(), fieldName);
        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(),
            CoreMatchers.equalTo("failed to parse field [" + fieldName + "] with value [106752d] as a time value"));
        assertThat(exception.getCause().getMessage(),
            CoreMatchers.equalTo("Values greater than 9223372036854775807 nanoseconds are not supported: 106752d"));
    }

    public void testTooSmall() {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "-106752d");
        Processor processor = newProcessor(fieldName, randomBoolean(), fieldName);
        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(),
            CoreMatchers.equalTo("failed to parse field [" + fieldName + "] with value [-106752d] as a time value"));
        assertThat(exception.getCause().getMessage(),
            CoreMatchers.equalTo("Values less than -9223372036854775808 nanoseconds are not supported: -106752d"));
    }

    public void testNotTimeValue() {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "junk");
        Processor processor = newProcessor(fieldName, randomBoolean(), fieldName);
        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(),
            CoreMatchers.equalTo("failed to parse field [" + fieldName + "] with value [junk] as a time value"));
        assertThat(exception.getCause().getMessage(),
            CoreMatchers.equalTo("failed to parse setting [" + fieldName +
                "] with value [junk] as a time value: unit is missing or unrecognized"));
    }

    public void testMissingUnits() {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "1");
        Processor processor = newProcessor(fieldName, randomBoolean(), fieldName);
        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(),
            CoreMatchers.equalTo("failed to parse field [" + fieldName + "] with value [1] as a time value"));
        assertThat(exception.getCause().getMessage(),
            CoreMatchers.equalTo("failed to parse setting [" + fieldName +
                "] with value [1] as a time value: unit is missing or unrecognized"));
    }

    public void testFractional() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "1.5d");
        Processor processor = newProcessor(fieldName, randomBoolean(), fieldName);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, expectedResultType()), CoreMatchers.equalTo(129600000L));
    }

    public void testFractionalConversion() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "7.8s");
        Processor processor = newProcessor(fieldName, randomBoolean(), fieldName, TimeUnit.NANOSECONDS);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, expectedResultType()), CoreMatchers.equalTo(7800000000L));
    }

    public void testFractionalTooLarge() {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "106751.991167302d");
        Processor processor = newProcessor(fieldName, randomBoolean(), fieldName);
        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(),
            CoreMatchers.equalTo("failed to parse field [" + fieldName + "] with value [106751.991167302d] as a time value"));
        assertThat(exception.getCause().getMessage(),
            CoreMatchers.equalTo("Values greater than 9223372036854775807 nanoseconds are not supported: 106751.991167302d"));
    }

    public void testFractionalTooSmall() {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "-106751.991167302d");
        Processor processor = newProcessor(fieldName, randomBoolean(), fieldName);
        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(),
            CoreMatchers.equalTo("failed to parse field [" + fieldName + "] with value [-106751.991167302d] as a time value"));
        assertThat(exception.getCause().getMessage(),
            CoreMatchers.equalTo("Values less than -9223372036854775808 nanoseconds are not supported: -106751.991167302d"));
    }
}
