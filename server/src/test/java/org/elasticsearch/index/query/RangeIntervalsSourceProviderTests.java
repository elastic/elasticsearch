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

import static org.elasticsearch.index.query.IntervalsSourceProvider.Range;

public class RangeIntervalsSourceProviderTests extends AbstractXContentSerializingTestCase<Range> {

    @Override
    protected Range createTestInstance() {
        return createRandomRange();
    }

    static Range createRandomRange() {
        return new Range(
            "a" + randomAlphaOfLengthBetween(1, 10),
            "z" + randomAlphaOfLengthBetween(1, 10),
            randomBoolean(),
            randomBoolean(),
            randomBoolean() ? randomAlphaOfLength(10) : null,
            randomBoolean() ? randomAlphaOfLength(10) : null
        );
    }

    @Override
    protected Range mutateInstance(Range instance) {
        String lowerTerm = instance.getLowerTerm();
        String upperTerm = instance.getUpperTerm();
        boolean includeLower = instance.getIncludeLower();
        boolean includeUpper = instance.getIncludeUpper();
        String analyzer = instance.getAnalyzer();
        String useField = instance.getUseField();
        switch (between(0, 5)) {
            case 0 -> lowerTerm = "a" + lowerTerm;
            case 1 -> upperTerm = "z" + upperTerm;
            case 2 -> includeLower = includeLower == false;
            case 3 -> includeUpper = includeUpper == false;
            case 4 -> analyzer = randomAlphaOfLength(5);
            case 5 -> useField = useField == null ? randomAlphaOfLength(5) : null;
        }
        return new Range(lowerTerm, upperTerm, includeLower, includeUpper, analyzer, useField);
    }

    @Override
    protected Writeable.Reader<Range> instanceReader() {
        return Range::new;
    }

    @Override
    protected Range doParseInstance(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        Range range = (Range) IntervalsSourceProvider.fromXContent(parser);
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        return range;
    }
}
