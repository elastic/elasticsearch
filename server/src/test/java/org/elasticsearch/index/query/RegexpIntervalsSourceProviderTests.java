/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.index.query.IntervalsSourceProvider.Regexp;

public class RegexpIntervalsSourceProviderTests extends AbstractXContentSerializingTestCase<Regexp> {

    @Override
    protected Regexp createTestInstance() {
        return createRandomRegexp();
    }

    static Regexp createRandomRegexp() {
        return new Regexp(
            randomAlphaOfLengthBetween(1, 10),
            randomBoolean() ? randomAlphaOfLength(10) : null,
            randomBoolean() ? randomAlphaOfLength(10) : null
        );
    }

    @Override
    protected Regexp mutateInstance(Regexp instance) {
        String regexp = instance.getPattern();
        String analyzer = instance.getAnalyzer();
        String useField = instance.getUseField();
        switch (between(0, 2)) {
            case 0 -> regexp += "a";
            case 1 -> analyzer = randomAlphaOfLength(5);
            case 2 -> useField = useField == null ? randomAlphaOfLength(5) : null;
        }
        return new Regexp(regexp, analyzer, useField);
    }

    @Override
    protected Writeable.Reader<Regexp> instanceReader() {
        return Regexp::new;
    }

    @Override
    protected Regexp doParseInstance(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        Regexp regexp = (Regexp) IntervalsSourceProvider.fromXContent(parser);
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        return regexp;
    }
}
