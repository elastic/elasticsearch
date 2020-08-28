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
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.filestructurefinder.FileStructure;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

public class FindFileStructureRequestTests extends AbstractXContentTestCase<FindFileStructureRequest> {

    private static final ObjectParser<FindFileStructureRequest, Void> PARSER =
        new ObjectParser<>("find_file_structure_request", FindFileStructureRequest::new);

    static {
        PARSER.declareInt(FindFileStructureRequest::setLinesToSample, FindFileStructureRequest.LINES_TO_SAMPLE);
        PARSER.declareInt(FindFileStructureRequest::setLineMergeSizeLimit, FindFileStructureRequest.LINE_MERGE_SIZE_LIMIT);
        PARSER.declareString((p, c) -> p.setTimeout(TimeValue.parseTimeValue(c, FindFileStructureRequest.TIMEOUT.getPreferredName())),
            FindFileStructureRequest.TIMEOUT);
        PARSER.declareString(FindFileStructureRequest::setCharset, FindFileStructureRequest.CHARSET);
        PARSER.declareString(FindFileStructureRequest::setFormat, FindFileStructureRequest.FORMAT);
        PARSER.declareStringArray(FindFileStructureRequest::setColumnNames, FindFileStructureRequest.COLUMN_NAMES);
        PARSER.declareBoolean(FindFileStructureRequest::setHasHeaderRow, FindFileStructureRequest.HAS_HEADER_ROW);
        PARSER.declareString(FindFileStructureRequest::setDelimiter, FindFileStructureRequest.DELIMITER);
        PARSER.declareString(FindFileStructureRequest::setQuote, FindFileStructureRequest.QUOTE);
        PARSER.declareBoolean(FindFileStructureRequest::setShouldTrimFields, FindFileStructureRequest.SHOULD_TRIM_FIELDS);
        PARSER.declareString(FindFileStructureRequest::setGrokPattern, FindFileStructureRequest.GROK_PATTERN);
        PARSER.declareString(FindFileStructureRequest::setTimestampFormat, FindFileStructureRequest.TIMESTAMP_FORMAT);
        PARSER.declareString(FindFileStructureRequest::setTimestampField, FindFileStructureRequest.TIMESTAMP_FIELD);
        PARSER.declareBoolean(FindFileStructureRequest::setExplain, FindFileStructureRequest.EXPLAIN);
        // Sample is not included in the X-Content representation
    }

    @Override
    protected FindFileStructureRequest doParseInstance(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected FindFileStructureRequest createTestInstance() {
        return createTestRequestWithoutSample();
    }

    public static FindFileStructureRequest createTestRequestWithoutSample() {

        FindFileStructureRequest findFileStructureRequest = new FindFileStructureRequest();
        if (randomBoolean()) {
            findFileStructureRequest.setLinesToSample(randomIntBetween(1000, 2000));
        }
        if (randomBoolean()) {
            findFileStructureRequest.setLineMergeSizeLimit(randomIntBetween(10000, 20000));
        }
        if (randomBoolean()) {
            findFileStructureRequest.setTimeout(TimeValue.timeValueSeconds(randomIntBetween(10, 20)));
        }
        if (randomBoolean()) {
            findFileStructureRequest.setCharset(Charset.defaultCharset().toString());
        }
        if (randomBoolean()) {
            findFileStructureRequest.setFormat(randomFrom(FileStructure.Format.values()));
        }
        if (randomBoolean()) {
            findFileStructureRequest.setColumnNames(Arrays.asList(generateRandomStringArray(10, 10, false, false)));
        }
        if (randomBoolean()) {
            findFileStructureRequest.setHasHeaderRow(randomBoolean());
        }
        if (randomBoolean()) {
            findFileStructureRequest.setDelimiter(randomAlphaOfLength(1));
        }
        if (randomBoolean()) {
            findFileStructureRequest.setQuote(randomAlphaOfLength(1));
        }
        if (randomBoolean()) {
            findFileStructureRequest.setShouldTrimFields(randomBoolean());
        }
        if (randomBoolean()) {
            findFileStructureRequest.setGrokPattern(randomAlphaOfLength(100));
        }
        if (randomBoolean()) {
            findFileStructureRequest.setTimestampFormat(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            findFileStructureRequest.setTimestampField(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            findFileStructureRequest.setExplain(randomBoolean());
        }

        return findFileStructureRequest;
    }
}
