/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.painless.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class PainlessExecuteResponseTests extends AbstractXContentSerializingTestCase<PainlessExecuteAction.Response> {

    @Override
    protected Writeable.Reader<PainlessExecuteAction.Response> instanceReader() {
        return PainlessExecuteAction.Response::new;
    }

    @Override
    protected PainlessExecuteAction.Response createTestInstance() {
        Object result = switch (randomIntBetween(0, 2)) {
            case 0 -> randomAlphaOfLength(10);
            case 1 -> randomBoolean();
            case 2 -> randomDoubleBetween(-10, 10, true);
            default -> throw new IllegalStateException("invalid branch");
        };
        return new PainlessExecuteAction.Response(result);
    }

    @Override
    protected PainlessExecuteAction.Response mutateInstance(PainlessExecuteAction.Response instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected PainlessExecuteAction.Response doParseInstance(XContentParser parser) throws IOException {
        parser.nextToken(); // START-OBJECT
        parser.nextToken(); // FIELD-NAME
        XContentParser.Token token = parser.nextToken(); // result value
        Object result = switch (token) {
            case VALUE_STRING -> parser.text();
            case VALUE_BOOLEAN -> parser.booleanValue();
            case VALUE_NUMBER -> parser.doubleValue();
            default -> throw new IOException("invalid response");
        };
        return new PainlessExecuteAction.Response(result);
    }
}
