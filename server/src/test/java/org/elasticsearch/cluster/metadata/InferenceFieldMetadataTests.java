/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;

public class InferenceFieldMetadataTests extends AbstractXContentTestCase<InferenceFieldMetadata> {

    public void testSerialization() throws IOException {
        final InferenceFieldMetadata before = createTestItem();
        final BytesStreamOutput out = new BytesStreamOutput();
        before.writeTo(out);

        final StreamInput in = out.bytes().streamInput();
        final InferenceFieldMetadata after = new InferenceFieldMetadata(in);

        assertThat(after, equalTo(before));
    }

    @Override
    protected InferenceFieldMetadata createTestInstance() {
        return createTestItem();
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return p -> p.equals(""); // do not add elements at the top-level as any element at this level is parsed as a new inference field
    }

    @Override
    protected InferenceFieldMetadata doParseInstance(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        assertEquals(XContentParser.Token.FIELD_NAME, parser.currentToken());
        InferenceFieldMetadata inferenceMetadata = InferenceFieldMetadata.fromXContent(parser);
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        return inferenceMetadata;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    private static InferenceFieldMetadata createTestItem() {
        String name = randomAlphaOfLengthBetween(3, 10);
        String inferenceId = randomIdentifier();
        String searchInferenceId = randomIdentifier();
        String[] inputFields = generateRandomStringArray(5, 10, false, false);
        return new InferenceFieldMetadata(name, inferenceId, searchInferenceId, inputFields);
    }

    public void testNullCtorArgsThrowException() {
        assertThrows(NullPointerException.class, () -> new InferenceFieldMetadata(null, "inferenceId", "searchInferenceId", new String[0]));
        assertThrows(NullPointerException.class, () -> new InferenceFieldMetadata("name", null, "searchInferenceId", new String[0]));
        assertThrows(NullPointerException.class, () -> new InferenceFieldMetadata("name", "inferenceId", null, new String[0]));
        assertThrows(NullPointerException.class, () -> new InferenceFieldMetadata("name", "inferenceId", "searchInferenceId", null));
    }
}
