/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

import static org.elasticsearch.index.query.IntervalsSourceProvider.Wildcard;

public class WildcardIntervalsSourceProviderTests extends AbstractSerializingTestCase<Wildcard> {

    @Override
    protected Wildcard createTestInstance() {
        return createRandomWildcard();
    }

    static Wildcard createRandomWildcard() {
        return new Wildcard(
            randomAlphaOfLength(10),
            randomBoolean() ? randomAlphaOfLength(10) : null,
            randomBoolean() ? randomAlphaOfLength(10) : null
        );
    }

    @Override
    protected Wildcard mutateInstance(Wildcard instance) throws IOException {
        String wildcard = instance.getPattern();
        String analyzer = instance.getAnalyzer();
        String useField = instance.getUseField();
        switch (between(0, 2)) {
            case 0:
                wildcard += "a";
                break;
            case 1:
                analyzer = randomAlphaOfLength(5);
                break;
            case 2:
                useField = useField == null ? randomAlphaOfLength(5) : null;
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new Wildcard(wildcard, analyzer, useField);
    }

    @Override
    protected Writeable.Reader<Wildcard> instanceReader() {
        return Wildcard::new;
    }

    @Override
    protected Wildcard doParseInstance(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        Wildcard wildcard = (Wildcard) IntervalsSourceProvider.fromXContent(parser);
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        return wildcard;
    }
}
