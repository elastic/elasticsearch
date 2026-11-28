/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.lucene.query.TsidPrefixRange;
import org.elasticsearch.compute.lucene.query.TsidPrefixRangeQuery;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.hamcrest.Matchers.equalTo;

public class TsidPrefixRangeQueryBuilderTests extends AbstractWireSerializingTestCase<TsidPrefixRangeQueryBuilder> {

    @Override
    protected Writeable.Reader<TsidPrefixRangeQueryBuilder> instanceReader() {
        return TsidPrefixRangeQueryBuilder::new;
    }

    @Override
    protected TsidPrefixRangeQueryBuilder createTestInstance() {
        return new TsidPrefixRangeQueryBuilder(randomRange());
    }

    @Override
    protected TsidPrefixRangeQueryBuilder mutateInstance(TsidPrefixRangeQueryBuilder instance) {
        return new TsidPrefixRangeQueryBuilder(randomValueOtherThan(instance.range(), TsidPrefixRangeQueryBuilderTests::randomRange));
    }

    public void testToQuery() {
        TsidPrefixRange range = randomRange();
        assertThat(new TsidPrefixRangeQueryBuilder(range).doToQuery(null), equalTo(new TsidPrefixRangeQuery(range)));
    }

    private static TsidPrefixRange randomRange() {
        int prefixBits = between(1, 30);
        int lower = between(0, (1 << prefixBits) - 1);
        return new TsidPrefixRange(prefixBits, lower, between(lower + 1, 1 << prefixBits));
    }
}
