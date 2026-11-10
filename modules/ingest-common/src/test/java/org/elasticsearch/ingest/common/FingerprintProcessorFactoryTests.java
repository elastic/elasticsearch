/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.TestIngestDocument;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class FingerprintProcessorFactoryTests extends ESTestCase {

    private FingerprintProcessor.Factory factory;

    @Before
    public void init() {
        factory = new FingerprintProcessor.Factory();
    }

    public void testCreate() throws Exception {
        Map<String, Object> config = new HashMap<>();
        List<String> fieldList = randomList(1, 10, () -> randomAlphaOfLength(8));
        List<String> sortedFieldList = new ArrayList<>(fieldList);
        sortedFieldList.sort(Comparator.naturalOrder());
        config.put("fields", fieldList);
        String targetField = randomAlphaOfLength(6);
        config.put("target_field", targetField);
        String salt = randomAlphaOfLength(6);
        config.put("salt", salt);
        String method = randomFrom(FingerprintProcessor.Factory.SUPPORTED_DIGESTS);
        config.put("method", method);
        boolean ignoreMissing = randomBoolean();
        config.put("ignore_missing", ignoreMissing);

        String processorTag = randomAlphaOfLength(10);
        FingerprintProcessor fingerprintProcessor = factory.create(null, processorTag, null, config, null);
        assertThat(fingerprintProcessor.getTag(), equalTo(processorTag));
        assertThat(fingerprintProcessor.getFields(), equalTo(sortedFieldList));
        assertThat(fingerprintProcessor.getTargetField(), hasToString(targetField));
        assertThat(fingerprintProcessor.getSalt(), equalTo(salt.getBytes(StandardCharsets.UTF_8)));
        assertThat(fingerprintProcessor.getThreadLocalHasher().get().getAlgorithm(), equalTo(method));
        assertThat(fingerprintProcessor.isIgnoreMissing(), equalTo(ignoreMissing));
    }

    public void testMethod() throws Exception {
        // valid method
        Map<String, Object> config = new HashMap<>();
        List<String> fieldList = randomList(1, 10, () -> randomAlphaOfLength(8));
        List<String> sortedFieldList = new ArrayList<>(fieldList);
        sortedFieldList.sort(Comparator.naturalOrder());
        config.put("fields", fieldList);
        String method = randomFrom(FingerprintProcessor.Factory.SUPPORTED_DIGESTS);
        config.put("method", method);

        String processorTag = randomAlphaOfLength(10);
        FingerprintProcessor fingerprintProcessor = factory.create(null, processorTag, null, config, null);
        assertThat(fingerprintProcessor.getTag(), equalTo(processorTag));
        assertThat(fingerprintProcessor.getFields(), equalTo(sortedFieldList));
        assertThat(fingerprintProcessor.getThreadLocalHasher().get().getAlgorithm(), equalTo(method));

        // invalid method
        String invalidMethod = randomValueOtherThanMany(
            m -> List.of(FingerprintProcessor.Factory.SUPPORTED_DIGESTS).contains(m),
            () -> randomAlphaOfLengthBetween(5, 9)
        );
        config.put("fields", fieldList);
        config.put("method", invalidMethod);
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> factory.create(null, processorTag, null, config, null));
        assertThat(e.getMessage(), containsString("[" + invalidMethod + "] must be one of the supported hash methods ["));
    }

    public void testFields() throws Exception {
        // valid fields
        Map<String, Object> config = new HashMap<>();
        List<String> fieldList = randomList(1, 10, () -> randomAlphaOfLength(8));
        List<String> sortedFieldList = new ArrayList<>(fieldList);
        sortedFieldList.sort(Comparator.naturalOrder());
        config.put("fields", fieldList);

        String processorTag = randomAlphaOfLength(10);
        FingerprintProcessor fingerprintProcessor = factory.create(null, processorTag, null, config, null);
        assertThat(fingerprintProcessor.getTag(), equalTo(processorTag));
        assertThat(fingerprintProcessor.getFields(), equalTo(sortedFieldList));

        // fields is a list of length zero
        config.put("fields", List.of());
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> factory.create(null, processorTag, null, config, null));
        assertThat(e.getMessage(), containsString("must specify at least one field"));

        // fields is missing
        e = expectThrows(ElasticsearchException.class, () -> factory.create(null, processorTag, null, config, null));
        assertThat(e.getMessage(), containsString("[fields] required property is missing"));
    }

    public void testDefaults() throws Exception {
        String processorTag = randomAlphaOfLength(10);
        List<String> fieldList = randomList(1, 10, () -> randomAlphaOfLength(8));
        List<String> sortedFieldList = new ArrayList<>(fieldList);
        sortedFieldList.sort(Comparator.naturalOrder());
        HashMap<String, Object> config = new HashMap<>();
        config.put("fields", fieldList);

        FingerprintProcessor fingerprintProcessor = factory.create(null, processorTag, null, config, null);
        assertThat(fingerprintProcessor.getTag(), equalTo(processorTag));
        assertThat(fingerprintProcessor.getFields(), equalTo(sortedFieldList));
        assertThat(fingerprintProcessor.getTargetField(), hasToString(FingerprintProcessor.Factory.DEFAULT_TARGET));
        assertThat(fingerprintProcessor.getSalt(), equalTo(new byte[0]));
        assertThat(fingerprintProcessor.getThreadLocalHasher().get().getAlgorithm(), equalTo(FingerprintProcessor.Factory.DEFAULT_METHOD));
        assertThat(fingerprintProcessor.isIgnoreMissing(), equalTo(false));
    }

    /**
     * Pipelines are stored, so a malformed path in one has to be accepted when the processor is created, with the failure only coming
     * when a document is run through it. This goes through the factory, as creating a processor from a stored pipeline does.
     */
    public void testCreateWithMalformedTargetField() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("fields", List.of("source"));
        config.put("target_field", "a..b");
        FingerprintProcessor processor = factory.create(null, randomAlphaOfLength(10), null, config, null);
        assertThat(processor.getTargetField(), hasToString("a..b"));

        // the source is fine, so it is the target that causes the failure
        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        ingestDocument.setFieldValue("source", "value");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("path [a..b] is not valid"));
    }
}
