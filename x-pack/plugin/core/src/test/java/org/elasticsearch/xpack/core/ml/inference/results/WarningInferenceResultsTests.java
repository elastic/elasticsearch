/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.HashMap;

import static org.hamcrest.Matchers.equalTo;

public class WarningInferenceResultsTests extends AbstractWireSerializingTestCase<WarningInferenceResults> {

    public static WarningInferenceResults createRandomResults() {
        return new WarningInferenceResults(randomAlphaOfLength(10));
    }

    public void testWriteResults() {
        WarningInferenceResults result = new WarningInferenceResults("foo");
        IngestDocument document = new IngestDocument(new HashMap<>(), new HashMap<>());
        result.writeResult(document, "result_field");

        assertThat(document.getFieldValue("result_field.warning", String.class), equalTo("foo"));
    }

    @Override
    protected WarningInferenceResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected Writeable.Reader<WarningInferenceResults> instanceReader() {
        return WarningInferenceResults::new;
    }
}
