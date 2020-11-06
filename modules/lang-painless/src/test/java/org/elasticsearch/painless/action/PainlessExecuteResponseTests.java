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
package org.elasticsearch.painless.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class PainlessExecuteResponseTests extends AbstractSerializingTestCase<PainlessExecuteAction.Response> {

    @Override
    protected Writeable.Reader<PainlessExecuteAction.Response> instanceReader() {
        return PainlessExecuteAction.Response::new;
    }

    @Override
    protected PainlessExecuteAction.Response createTestInstance() {
        Object result;
        switch (randomIntBetween(0, 2)) {
            case 0:
                result = randomAlphaOfLength(10);
                break;
            case 1:
                result = randomBoolean();
                break;
            case 2:
                result = randomDoubleBetween(-10, 10, true);
                break;
            default:
                throw new IllegalStateException("invalid branch");
        }
        return new PainlessExecuteAction.Response(result);
    }

    @Override
    protected PainlessExecuteAction.Response doParseInstance(XContentParser parser) throws IOException {
        parser.nextToken(); // START-OBJECT
        parser.nextToken(); // FIELD-NAME
        XContentParser.Token token = parser.nextToken(); // result value
        Object result;
        switch (token) {
            case VALUE_STRING:
                result = parser.text();
                break;
            case VALUE_BOOLEAN:
                result = parser.booleanValue();
                break;
            case VALUE_NUMBER:
                result = parser.doubleValue();
                break;
            default:
                throw new IOException("invalid response");
        }
        return new PainlessExecuteAction.Response(result);
    }
}
