/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.index.query.IntervalsSourceProvider.Prefix;

public class PrefixIntervalsSourceProviderTests extends AbstractSerializingTestCase<Prefix> {

    @Override
    protected Prefix createTestInstance() {
        return new Prefix(
            randomAlphaOfLength(10),
            randomBoolean() ? randomAlphaOfLength(10) : null,
            randomBoolean() ? randomAlphaOfLength(10) : null
        );
    }

    @Override
    protected Prefix mutateInstance(Prefix instance) throws IOException {
        String prefix = instance.getPrefix();
        String analyzer = instance.getAnalyzer();
        String useField = instance.getUseField();
        switch (between(0, 2)) {
            case 0 -> prefix += "a";
            case 1 -> analyzer = randomAlphaOfLength(5);
            case 2 -> useField = useField == null ? randomAlphaOfLength(5) : null;
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new Prefix(prefix, analyzer, useField);
    }

    @Override
    protected Writeable.Reader<Prefix> instanceReader() {
        return Prefix::new;
    }

    @Override
    protected Prefix doParseInstance(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        Prefix prefix = (Prefix) IntervalsSourceProvider.fromXContent(parser);
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        return prefix;
    }
}
