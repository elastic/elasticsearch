/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action.util;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xpack.ml.job.results.Influencer;
import org.elasticsearch.xpack.ml.support.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.Date;

public class QueryPageTests extends AbstractWireSerializingTestCase<QueryPage<Influencer>> {

    @Override
    protected QueryPage<Influencer> createTestInstance() {
        int hitCount = randomIntBetween(0, 10);
        ArrayList<Influencer> hits = new ArrayList<>();
        for (int i = 0; i < hitCount; i++) {
            hits.add(new Influencer(randomAsciiOfLengthBetween(1, 20), randomAsciiOfLengthBetween(1, 20),
                    randomAsciiOfLengthBetween(1, 20), new Date(), randomNonNegativeLong(), i + 1));
        }
        return new QueryPage<>(hits, hitCount, new ParseField("test"));
    }

    @Override
    protected Reader<QueryPage<Influencer>> instanceReader() {
        return (in) -> new QueryPage<>(in, Influencer::new);
    }
}
