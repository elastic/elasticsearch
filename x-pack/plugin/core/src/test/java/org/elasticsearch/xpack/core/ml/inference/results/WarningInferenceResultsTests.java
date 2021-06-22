/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xpack.core.ml.inference.results.InferenceResults.writeResult;
import static org.hamcrest.Matchers.equalTo;

public class WarningInferenceResultsTests extends AbstractSerializingTestCase<WarningInferenceResults> {

    private static final ConstructingObjectParser<WarningInferenceResults, Void> PARSER =
        new ConstructingObjectParser<>("inference_warning",
            a -> new WarningInferenceResults((String) a[0])
        );

    static {
        PARSER.declareString(constructorArg(), new ParseField(WarningInferenceResults.NAME));
    }

    public static WarningInferenceResults createRandomResults() {
        return new WarningInferenceResults(randomAlphaOfLength(10));
    }

    public void testWriteResults() {
        WarningInferenceResults result = new WarningInferenceResults("foo");
        IngestDocument document = new IngestDocument(new HashMap<>(), new HashMap<>());
        writeResult(result, document, "result_field", "test");

        assertThat(document.getFieldValue("result_field.warning", String.class), equalTo("foo"));

        result = new WarningInferenceResults("bar");
        writeResult(result, document, "result_field", "test");

        assertThat(document.getFieldValue("result_field.0.warning", String.class), equalTo("foo"));
        assertThat(document.getFieldValue("result_field.1.warning", String.class), equalTo("bar"));
    }

    @Override
    protected WarningInferenceResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected Writeable.Reader<WarningInferenceResults> instanceReader() {
        return WarningInferenceResults::new;
    }

    @Override
    protected WarningInferenceResults doParseInstance(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }
}
