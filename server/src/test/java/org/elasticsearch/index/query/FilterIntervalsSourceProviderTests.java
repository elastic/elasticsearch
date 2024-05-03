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
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.index.query.IntervalsSourceProvider.IntervalFilter;

public class FilterIntervalsSourceProviderTests extends AbstractXContentSerializingTestCase<IntervalFilter> {

    @Override
    protected IntervalFilter createTestInstance() {
        return IntervalQueryBuilderTests.createRandomNonNullFilter(0, randomBoolean());
    }

    @Override
    protected IntervalFilter mutateInstance(IntervalFilter instance) {
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
