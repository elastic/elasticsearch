/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.textstructure.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;

import java.util.Arrays;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

public class FindTextStructureActionRequestTests extends AbstractWireSerializingTestCase<FindStructureAction.Request> {

    @Override
    protected FindStructureAction.Request createTestInstance() {

        FindStructureAction.Request request = new FindStructureAction.Request();

        if (randomBoolean()) {
            request.setLinesToSample(randomIntBetween(10, 2000));
        }

        if (randomBoolean()) {
            request.setLineMergeSizeLimit(randomIntBetween(1000, 20000));
        }

        if (randomBoolean()) {
            request.setCharset(randomAlphaOfLength(10));
        }

        if (randomBoolean()) {
            TextStructure.Format format = randomFrom(TextStructure.Format.values());
            request.setFormat(format);
            if (format == TextStructure.Format.DELIMITED) {
                if (randomBoolean()) {
                    request.setColumnNames(generateRandomStringArray(10, 15, false, false));
                }
                if (randomBoolean()) {
                    request.setHasHeaderRow(randomBoolean());
                }
                if (randomBoolean()) {
                    request.setDelimiter(randomFrom(',', '\t', ';', '|'));
                }
                if (randomBoolean()) {
                    request.setQuote(randomFrom('"', '\''));
                }
                if (randomBoolean()) {
                    request.setShouldTrimFields(randomBoolean());
                }
            } else if (format == TextStructure.Format.SEMI_STRUCTURED_TEXT) {
                if (randomBoolean()) {
                    request.setGrokPattern(randomAlphaOfLength(80));
                }
            }
        }

        if (randomBoolean()) {
            request.setTimestampFormat(randomAlphaOfLength(20));
        }
        if (randomBoolean()) {
            request.setTimestampField(randomAlphaOfLength(15));
        }

        request.setSample(new BytesArray(randomByteArrayOfLength(randomIntBetween(1000, 20000))));

        return request;
    }

    @Override
    protected Writeable.Reader<FindStructureAction.Request> instanceReader() {
        return FindStructureAction.Request::new;
    }

    public void testValidateLinesToSample() {

        FindStructureAction.Request request = new FindStructureAction.Request();
        request.setLinesToSample(randomIntBetween(-1, 1));
        request.setSample(new BytesArray("foo\n"));

        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertThat(e.getMessage(), startsWith("Validation Failed: "));
        assertThat(e.getMessage(), containsString(" [lines_to_sample] must be at least [2] if specified"));
    }

    public void testValidateLineMergeSizeLimit() {

        FindStructureAction.Request request = new FindStructureAction.Request();
        request.setLineMergeSizeLimit(randomIntBetween(-1, 0));
        request.setSample(new BytesArray("foo\n"));

        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertThat(e.getMessage(), startsWith("Validation Failed: "));
        assertThat(e.getMessage(), containsString(" [line_merge_size_limit] must be positive if specified"));
    }

    public void testValidateNonDelimited() {

        FindStructureAction.Request request = new FindStructureAction.Request();
        String errorField;
        switch (randomIntBetween(0, 4)) {
            case 0:
                errorField = "column_names";
                request.setColumnNames(Arrays.asList("col1", "col2"));
                break;
            case 1:
                errorField = "has_header_row";
                request.setHasHeaderRow(randomBoolean());
                break;
            case 2:
                errorField = "delimiter";
                request.setDelimiter(randomFrom(',', '\t', ';', '|'));
                break;
            case 3:
                errorField = "quote";
                request.setQuote(randomFrom('"', '\''));
                break;
            case 4:
                errorField = "should_trim_fields";
                request.setShouldTrimFields(randomBoolean());
                break;
            default:
                throw new IllegalStateException("unexpected switch value");
        }
        request.setSample(new BytesArray("foo\n"));

        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertThat(e.getMessage(), startsWith("Validation Failed: "));
        assertThat(e.getMessage(), containsString(" [" + errorField + "] may only be specified if [format] is [delimited]"));
    }

    public void testValidateNonSemiStructuredText() {

        FindStructureAction.Request request = new FindStructureAction.Request();
        request.setFormat(randomFrom(TextStructure.Format.NDJSON, TextStructure.Format.XML, TextStructure.Format.DELIMITED));
        request.setGrokPattern(randomAlphaOfLength(80));
        request.setSample(new BytesArray("foo\n"));

        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertThat(e.getMessage(), startsWith("Validation Failed: "));
        assertThat(e.getMessage(), containsString(" [grok_pattern] may only be specified if [format] is [semi_structured_text]"));
    }

    public void testValidateSample() {

        FindStructureAction.Request request = new FindStructureAction.Request();
        if (randomBoolean()) {
            request.setSample(BytesArray.EMPTY);
        }

        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertThat(e.getMessage(), startsWith("Validation Failed: "));
        assertThat(e.getMessage(), containsString(" sample must be specified"));
    }
}
