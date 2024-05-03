/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.Arrays;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class AliasFilterTests extends ESTestCase {

    public void testEqualsAndHashCode() {
        final QueryBuilder filter = QueryBuilders.termQuery("field", "value");
        final String[] aliases = new String[] { "alias_0", "alias_1" };
        final AliasFilter aliasFilter = AliasFilter.of(filter, aliases);
        final EqualsHashCodeTestUtils.CopyFunction<AliasFilter> aliasFilterCopyFunction = x -> {
            assertThat(x.getQueryBuilder(), instanceOf(TermQueryBuilder.class));
            final BytesStreamOutput out = new BytesStreamOutput();
            x.getQueryBuilder().writeTo(out);
            final QueryBuilder otherFilter = new TermQueryBuilder(out.bytes().streamInput());
            final String[] otherAliases = Arrays.copyOf(x.getAliases(), x.getAliases().length);
            return AliasFilter.of(otherFilter, otherAliases);
        };

        final EqualsHashCodeTestUtils.MutateFunction<AliasFilter> aliasFilterMutationFunction = x -> {
            assertThat(x.getQueryBuilder(), instanceOf(TermQueryBuilder.class));
            final BytesStreamOutput out = new BytesStreamOutput();
            x.getQueryBuilder().writeTo(out);
            final QueryBuilder otherFilter = new TermQueryBuilder(out.bytes().streamInput());
            assertThat(x.getAliases().length, greaterThan(0));
            final String[] otherAliases = Arrays.copyOf(x.getAliases(), x.getAliases().length - 1);
            return AliasFilter.of(otherFilter, otherAliases);
        };

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(aliasFilter, aliasFilterCopyFunction, aliasFilterMutationFunction);
    }

}
