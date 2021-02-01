/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.textstructure;

import org.elasticsearch.client.textstructure.structurefinder.TextStructure;
import org.elasticsearch.common.unit.TimeValue;
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
