/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.index.query.IntervalsSourceProvider.Disjunction;

public class DisjunctionIntervalsSourceProviderTests extends AbstractXContentSerializingTestCase<Disjunction> {

    @Override
    protected Disjunction createTestInstance() {
        return IntervalQueryBuilderTests.createRandomDisjunction(0, randomBoolean());
    }

    @Override
    protected Disjunction mutateInstance(Disjunction instance) {
        List<IntervalsSourceProvider> subSources = instance.getSubSources();
        IntervalsSourceProvider.IntervalFilter filter = instance.getFilter();
        boolean rewrite = instance.getRewrite();
        switch (randomInt(2)) {
            case 0 -> subSources = subSources == null
                ? IntervalQueryBuilderTests.createRandomSourceList(0, randomBoolean(), randomInt(5) + 1)
                : null;
            case 1 -> filter = filter == null
                ? IntervalQueryBuilderTests.createRandomNonNullFilter(0, randomBoolean())
                : FilterIntervalsSourceProviderTests.mutateFilter(filter);
            case 2 -> rewrite = rewrite == false;
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new Disjunction(subSources, filter, rewrite);
    }

    @Override
    protected Writeable.Reader<Disjunction> instanceReader() {
        return Disjunction::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(SearchModule.getIntervalsSourceProviderNamedWritables());
    }

    @Override
    protected Disjunction doParseInstance(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        Disjunction disjunction = (Disjunction) IntervalsSourceProvider.fromXContent(parser);
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        return disjunction;
    }
}
