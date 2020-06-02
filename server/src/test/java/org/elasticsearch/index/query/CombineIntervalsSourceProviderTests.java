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
                ordered = !ordered;
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
