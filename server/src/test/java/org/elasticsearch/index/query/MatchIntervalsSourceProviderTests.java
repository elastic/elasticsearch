/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.index.query.IntervalsSourceProvider.Match;

public class MatchIntervalsSourceProviderTests extends AbstractXContentSerializingTestCase<Match> {

    @Override
    protected Match createTestInstance() {
        return IntervalQueryBuilderTests.createRandomMatch(0, randomBoolean());
    }

    @Override
    protected Match mutateInstance(Match instance) {
        String query = instance.getQuery();
        int maxGaps = instance.getMaxGaps();
        boolean isOrdered = instance.isOrdered();
        String analyzer = instance.getAnalyzer();
        IntervalsSourceProvider.IntervalFilter filter = instance.getFilter();
        String useField = instance.getUseField();
        switch (between(0, 5)) {
            case 0 -> query = randomAlphaOfLength(query.length() + 3);
            case 1 -> maxGaps++;
            case 2 -> isOrdered = isOrdered == false;
            case 3 -> analyzer = analyzer == null ? randomAlphaOfLength(5) : null;
            case 4 -> filter = filter == null
                ? IntervalQueryBuilderTests.createRandomNonNullFilter(0, randomBoolean())
                : FilterIntervalsSourceProviderTests.mutateFilter(filter);
            case 5 -> useField = useField == null ? randomAlphaOfLength(5) : (useField + "foo");
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new Match(query, maxGaps, isOrdered, analyzer, filter, useField);
    }

    @Override
    protected Writeable.Reader<Match> instanceReader() {
        return Match::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(SearchModule.getIntervalsSourceProviderNamedWritables());
    }

    @Override
    protected Match doParseInstance(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        Match Match = (Match) IntervalsSourceProvider.fromXContent(parser);
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        return Match;
    }
}
