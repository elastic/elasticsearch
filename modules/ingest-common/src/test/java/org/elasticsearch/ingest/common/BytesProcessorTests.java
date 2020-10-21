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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.hamcrest.CoreMatchers;

import static org.hamcrest.Matchers.equalTo;

public class BytesProcessorTests extends AbstractStringProcessorTestCase<Long> {

    private String modifiedInput;

    @Override
    protected AbstractStringProcessor<Long> newProcessor(String field, boolean ignoreMissing, String targetField) {
        return new BytesProcessor(randomAlphaOfLength(10), null, field, ignoreMissing, targetField);
    }

    @Override
    protected String modifyInput(String input) {
        //largest value that allows all results < Long.MAX_VALUE bytes
        long randomNumber = randomLongBetween(1, Long.MAX_VALUE / ByteSizeUnit.PB.toBytes(1));
        ByteSizeUnit randomUnit = randomFrom(ByteSizeUnit.values());
        modifiedInput = randomNumber + randomUnit.getSuffix();
        return modifiedInput;
    }

    @Override
    protected Long expectedResult(String input) {
        return ByteSizeValue.parseBytesSizeValue(modifiedInput, null, "").getBytes();
    }

    @Override
    protected Class<Long> expectedResultType() {
        return Long.class;
    }

    public void testTooLarge() {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "8912pb");
        Processor processor = newProcessor(fieldName, randomBoolean(), fieldName);
        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(),
            CoreMatchers.equalTo("failed to parse setting [Ingest Field] with value [8912pb] as a size in bytes"));
        assertThat(exception.getCause().getMessage(),
            CoreMatchers.containsString("Values greater than 9223372036854775807 bytes are not supported"));
    }

    public void testNotBytes() {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "junk");
        Processor processor = newProcessor(fieldName, randomBoolean(), fieldName);
        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(),
            CoreMatchers.equalTo("failed to parse [junk]"));
    }

    public void testMissingUnits() {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "1");
        Processor processor = newProcessor(fieldName, randomBoolean(), fieldName);
        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(),
            CoreMatchers.containsString("unit is missing or unrecognized"));
    }

    public void testFractional() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "1.1kb");
        Processor processor = newProcessor(fieldName, randomBoolean(), fieldName);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, expectedResultType()), equalTo(1126L));
        assertWarnings("Fractional bytes values are deprecated. Use non-fractional bytes values instead: [1.1kb] found for setting " +
            "[Ingest Field]");
    }
}
