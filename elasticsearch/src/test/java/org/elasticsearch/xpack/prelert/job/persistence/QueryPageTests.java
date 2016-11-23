/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xpack.prelert.job.results.Influencer;
import org.elasticsearch.xpack.prelert.support.AbstractWireSerializingTestCase;
import java.util.ArrayList;

public class QueryPageTests extends AbstractWireSerializingTestCase<QueryPage<Influencer>> {

    @Override
    protected QueryPage<Influencer> createTestInstance() {
        int hitCount = randomIntBetween(0, 10);
        ArrayList<Influencer> hits = new ArrayList<>();
        for (int i = 0; i < hitCount; i++) {
            hits.add(new Influencer(randomAsciiOfLengthBetween(1, 20), randomAsciiOfLengthBetween(1, 20),
                    randomAsciiOfLengthBetween(1, 20)));
        }
        return new QueryPage<>(hits, hitCount, new ParseField("test"));
    }

    @Override
    protected Reader<QueryPage<Influencer>> instanceReader() {
        return (in) -> new QueryPage<>(in, Influencer::new);
    }
}
