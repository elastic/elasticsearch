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

import static org.elasticsearch.index.query.IntervalsSourceProvider.Disjunction;

public class DisjunctionIntervalsSourceProviderTests extends AbstractSerializingTestCase<Disjunction> {

    @Override
    protected Disjunction createTestInstance() {
        return IntervalQueryBuilderTests.createRandomDisjunction(0, randomBoolean());
    }

    @Override
    protected Disjunction mutateInstance(Disjunction instance) throws IOException {
        List<IntervalsSourceProvider> subSources = instance.getSubSources();
        IntervalsSourceProvider.IntervalFilter filter = instance.getFilter();
        if (randomBoolean()) {
            subSources = subSources == null ?
                IntervalQueryBuilderTests.createRandomSourceList(0, randomBoolean(), randomInt(5) + 1) :
                null;
        } else {
            filter = filter == null ?
                IntervalQueryBuilderTests.createRandomNonNullFilter(0, randomBoolean()) :
                FilterIntervalsSourceProviderTests.mutateFilter(filter);
        }
        return new Disjunction(subSources, filter);
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
