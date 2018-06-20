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

package org.elasticsearch.action.admin.indices.analyze;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AnalyzeResponseTests extends AbstractStreamableXContentTestCase<AnalyzeResponse> {

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected AnalyzeResponse doParseInstance(XContentParser parser) throws IOException {
        return AnalyzeResponse.fromXContent(parser);
    }

    @Override
    protected AnalyzeResponse createBlankInstance() {
        return new AnalyzeResponse();
    }

    @Override
    protected AnalyzeResponse createTestInstance() {
        List<AnalyzeResponse.AnalyzeToken> tokens = new ArrayList<>();
        tokens.add(new AnalyzeResponse.AnalyzeToken("one", 0, 0, 3, 1, "<ALPHANUM>", Collections.emptyMap()));
        tokens.add(new AnalyzeResponse.AnalyzeToken("two", 1, 4, 7, 1, "<ALPHANUM>", Collections.emptyMap()));
        return new AnalyzeResponse(tokens, null);
    }
}
