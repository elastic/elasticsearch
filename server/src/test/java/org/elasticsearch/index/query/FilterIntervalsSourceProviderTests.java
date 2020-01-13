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
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.index.query.IntervalsSourceProvider.IntervalFilter;

public class FilterIntervalsSourceProviderTests extends AbstractSerializingTestCase<IntervalFilter> {

    @Override
    protected IntervalFilter createTestInstance() {
        return IntervalQueryBuilderTests.createRandomNonNullFilter(0, randomBoolean());
    }

    @Override
    protected IntervalFilter mutateInstance(IntervalFilter instance) throws IOException {
        return mutateFilter(instance);
    }

    static IntervalFilter mutateFilter(IntervalFilter instance) {
        IntervalsSourceProvider filter = instance.getFilter();
        String type = instance.getType();
        Script script = instance.getScript();

        if (filter != null) {
            if (randomBoolean()) {
                if (filter instanceof IntervalsSourceProvider.Match) {
                    filter = WildcardIntervalsSourceProviderTests.createRandomWildcard();
                } else {
                    filter = IntervalQueryBuilderTests.createRandomMatch(0, randomBoolean());
                }
            } else {
                if (type.equals("containing")) {
                    type = "overlapping";
                } else {
                    type = "containing";
                }
            }
            return new IntervalFilter(filter, type);
        } else {
            return new IntervalFilter(new Script(ScriptType.INLINE, "mockscript", script.getIdOrCode() + "foo", Collections.emptyMap()));
        }
    }

    @Override
    protected Writeable.Reader<IntervalFilter> instanceReader() {
        return IntervalFilter::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(SearchModule.getIntervalsSourceProviderNamedWritables());
    }

    @Override
    protected IntervalFilter doParseInstance(XContentParser parser) throws IOException {
        parser.nextToken();
        return IntervalFilter.fromXContent(parser);
    }
}
