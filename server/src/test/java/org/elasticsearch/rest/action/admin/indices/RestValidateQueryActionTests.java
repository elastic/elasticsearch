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
package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.HashMap;

public class RestValidateQueryActionTests extends ESTestCase {

    public void testRestValidateQueryAction_emptyQuery() {
        final String content = "{\"query\":{}}";

        expectParsingException(content);
    }

    public void testRestValidateQueryAction_malformed() {
        final String content = "{malformed_json}";

        expectParsingException(content);
    }

    private void expectParsingException(String content) {
        final RestRequest request = new FakeRestRequest
            .Builder(xContentRegistry())
            .withPath("index1/type1/_validate/query")
            .withParams(new HashMap<>())
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();

        final ValidateQueryRequest validateQueryRequest = new ValidateQueryRequest("index1");

        expectThrows(ParsingException.class,
            () -> request.withContentOrSourceParamParserOrNull(parser -> validateQueryRequest.query(RestActions.getQueryContent(parser))));
    }
}
