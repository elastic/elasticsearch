/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
