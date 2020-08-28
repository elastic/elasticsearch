/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.filestructurefinder.FileStructure;

import java.util.Arrays;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

public class FindFileStructureActionRequestTests extends AbstractWireSerializingTestCase<FindFileStructureAction.Request> {

    @Override
    protected FindFileStructureAction.Request createTestInstance() {

        FindFileStructureAction.Request request = new FindFileStructureAction.Request();

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
            FileStructure.Format format = randomFrom(FileStructure.Format.values());
            request.setFormat(format);
            if (format == FileStructure.Format.DELIMITED) {
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
            } else if (format == FileStructure.Format.SEMI_STRUCTURED_TEXT) {
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
    protected Writeable.Reader<FindFileStructureAction.Request> instanceReader() {
        return FindFileStructureAction.Request::new;
    }

    public void testValidateLinesToSample() {

        FindFileStructureAction.Request request = new FindFileStructureAction.Request();
        request.setLinesToSample(randomIntBetween(-1, 0));
        request.setSample(new BytesArray("foo\n"));

        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertThat(e.getMessage(), startsWith("Validation Failed: "));
        assertThat(e.getMessage(), containsString(" [lines_to_sample] must be positive if specified"));
    }

    public void testValidateLineMergeSizeLimit() {

        FindFileStructureAction.Request request = new FindFileStructureAction.Request();
        request.setLineMergeSizeLimit(randomIntBetween(-1, 0));
        request.setSample(new BytesArray("foo\n"));

        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertThat(e.getMessage(), startsWith("Validation Failed: "));
        assertThat(e.getMessage(), containsString(" [line_merge_size_limit] must be positive if specified"));
    }

    public void testValidateNonDelimited() {

        FindFileStructureAction.Request request = new FindFileStructureAction.Request();
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

        FindFileStructureAction.Request request = new FindFileStructureAction.Request();
        request.setFormat(randomFrom(FileStructure.Format.NDJSON, FileStructure.Format.XML, FileStructure.Format.DELIMITED));
        request.setGrokPattern(randomAlphaOfLength(80));
        request.setSample(new BytesArray("foo\n"));

        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertThat(e.getMessage(), startsWith("Validation Failed: "));
        assertThat(e.getMessage(), containsString(" [grok_pattern] may only be specified if [format] is [semi_structured_text]"));
    }

    public void testValidateSample() {

        FindFileStructureAction.Request request = new FindFileStructureAction.Request();
        if (randomBoolean()) {
            request.setSample(BytesArray.EMPTY);
        }

        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertThat(e.getMessage(), startsWith("Validation Failed: "));
        assertThat(e.getMessage(), containsString(" sample must be specified"));
    }
}
