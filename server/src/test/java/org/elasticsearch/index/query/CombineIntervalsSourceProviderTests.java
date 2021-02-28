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
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.index.query.IntervalsSourceProvider.Combine;

public class CombineIntervalsSourceProviderTests extends AbstractSerializingTestCase<Combine> {

    @Override
    protected Combine createTestInstance() {
        return IntervalQueryBuilderTests.createRandomCombine(0, randomBoolean());
    }

    @Override
    protected Combine mutateInstance(Combine instance) throws IOException {
        List<IntervalsSourceProvider> subSources = instance.getSubSources();
        boolean ordered = instance.isOrdered();
        int maxGaps = instance.getMaxGaps();
        IntervalsSourceProvider.IntervalFilter filter = instance.getFilter();
        switch (between(0, 3)) {
            case 0:
                subSources = subSources == null ?
                    IntervalQueryBuilderTests.createRandomSourceList(0, randomBoolean(), randomInt(5) + 1) :
                    null;
                break;
            case 1:
                ordered = ordered == false;
                break;
            case 2:
                maxGaps++;
                break;
            case 3:
                filter = filter == null ?
                    IntervalQueryBuilderTests.createRandomNonNullFilter(0, randomBoolean()) :
                    FilterIntervalsSourceProviderTests.mutateFilter(filter);
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new Combine(subSources, ordered, maxGaps, filter);
    }

    @Override
    protected Writeable.Reader<Combine> instanceReader() {
        return Combine::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(SearchModule.getIntervalsSourceProviderNamedWritables());
    }

    @Override
    protected Combine doParseInstance(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        Combine combine = (Combine) IntervalsSourceProvider.fromXContent(parser);
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        return combine;
    }
}
