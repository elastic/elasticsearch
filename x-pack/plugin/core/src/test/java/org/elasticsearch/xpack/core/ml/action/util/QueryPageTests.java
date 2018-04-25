/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action.util;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.job.results.Influencer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class QueryPageTests extends AbstractWireSerializingTestCase<QueryPage<Influencer>> {

    @Override
    protected QueryPage<Influencer> createTestInstance() {
        int hitCount = randomIntBetween(0, 10);
        ArrayList<Influencer> hits = new ArrayList<>();
        for (int i = 0; i < hitCount; i++) {
            hits.add(new Influencer(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20),
                    randomAlphaOfLengthBetween(1, 20), new Date(), randomNonNegativeLong()));
        }
        return new QueryPage<>(hits, hitCount, new ParseField("test"));
    }

    @Override
    protected Reader<QueryPage<Influencer>> instanceReader() {
        return (in) -> new QueryPage<>(in, Influencer::new);
    }

    @Override
    protected QueryPage<Influencer> mutateInstance(QueryPage<Influencer> instance) throws IOException {
        ParseField resultsField = instance.getResultsField();
        List<Influencer> page = instance.results();
        long count = instance.count();
        switch (between(0, 1)) {
        case 0:
            page = new ArrayList<>(page);
            page.add(new Influencer(randomAlphaOfLengthBetween(10, 20), randomAlphaOfLengthBetween(10, 20),
                    randomAlphaOfLengthBetween(10, 20), new Date(randomNonNegativeLong()), randomNonNegativeLong()));
            break;
        case 1:
            count += between(1, 20);
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new QueryPage<>(page, count, resultsField);
    }
}
