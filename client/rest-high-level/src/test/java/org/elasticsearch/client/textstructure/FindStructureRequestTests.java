/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.textstructure;

import org.elasticsearch.client.textstructure.structurefinder.TextStructure;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

public class FindStructureRequestTests extends AbstractXContentTestCase<FindStructureRequest> {

    private static final ObjectParser<FindStructureRequest, Void> PARSER =
        new ObjectParser<>("find_file_structure_request", FindStructureRequest::new);

    static {
        PARSER.declareInt(FindStructureRequest::setLinesToSample, FindStructureRequest.LINES_TO_SAMPLE);
        PARSER.declareInt(FindStructureRequest::setLineMergeSizeLimit, FindStructureRequest.LINE_MERGE_SIZE_LIMIT);
        PARSER.declareString((p, c) -> p.setTimeout(TimeValue.parseTimeValue(c, FindStructureRequest.TIMEOUT.getPreferredName())),
            FindStructureRequest.TIMEOUT);
        PARSER.declareString(FindStructureRequest::setCharset, FindStructureRequest.CHARSET);
        PARSER.declareString(FindStructureRequest::setFormat, FindStructureRequest.FORMAT);
        PARSER.declareStringArray(FindStructureRequest::setColumnNames, FindStructureRequest.COLUMN_NAMES);
        PARSER.declareBoolean(FindStructureRequest::setHasHeaderRow, FindStructureRequest.HAS_HEADER_ROW);
        PARSER.declareString(FindStructureRequest::setDelimiter, FindStructureRequest.DELIMITER);
        PARSER.declareString(FindStructureRequest::setQuote, FindStructureRequest.QUOTE);
        PARSER.declareBoolean(FindStructureRequest::setShouldTrimFields, FindStructureRequest.SHOULD_TRIM_FIELDS);
        PARSER.declareString(FindStructureRequest::setGrokPattern, FindStructureRequest.GROK_PATTERN);
        PARSER.declareString(FindStructureRequest::setTimestampFormat, FindStructureRequest.TIMESTAMP_FORMAT);
        PARSER.declareString(FindStructureRequest::setTimestampField, FindStructureRequest.TIMESTAMP_FIELD);
        PARSER.declareBoolean(FindStructureRequest::setExplain, FindStructureRequest.EXPLAIN);
        // Sample is not included in the X-Content representation
    }

    @Override
    protected FindStructureRequest doParseInstance(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected FindStructureRequest createTestInstance() {
        return createTestRequestWithoutSample();
    }

    public static FindStructureRequest createTestRequestWithoutSample() {

        FindStructureRequest findStructureRequest = new FindStructureRequest();
        if (randomBoolean()) {
            findStructureRequest.setLinesToSample(randomIntBetween(1000, 2000));
        }
        if (randomBoolean()) {
            findStructureRequest.setLineMergeSizeLimit(randomIntBetween(10000, 20000));
        }
        if (randomBoolean()) {
            findStructureRequest.setTimeout(TimeValue.timeValueSeconds(randomIntBetween(10, 20)));
        }
        if (randomBoolean()) {
            findStructureRequest.setCharset(Charset.defaultCharset().toString());
        }
        if (randomBoolean()) {
            findStructureRequest.setFormat(randomFrom(TextStructure.Format.values()));
        }
        if (randomBoolean()) {
            findStructureRequest.setColumnNames(Arrays.asList(generateRandomStringArray(10, 10, false, false)));
        }
        if (randomBoolean()) {
            findStructureRequest.setHasHeaderRow(randomBoolean());
        }
        if (randomBoolean()) {
            findStructureRequest.setDelimiter(randomAlphaOfLength(1));
        }
        if (randomBoolean()) {
            findStructureRequest.setQuote(randomAlphaOfLength(1));
        }
        if (randomBoolean()) {
            findStructureRequest.setShouldTrimFields(randomBoolean());
        }
        if (randomBoolean()) {
            findStructureRequest.setGrokPattern(randomAlphaOfLength(100));
        }
        if (randomBoolean()) {
            findStructureRequest.setTimestampFormat(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            findStructureRequest.setTimestampField(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            findStructureRequest.setExplain(randomBoolean());
        }

        return findStructureRequest;
    }
}
