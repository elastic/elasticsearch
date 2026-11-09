/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.TDigestBlock;
import org.elasticsearch.compute.data.TDigestBlockBuilder;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.spill.QueryScope;
import org.elasticsearch.compute.spill.ResultCollector;
import org.elasticsearch.compute.spill.ResultPages;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.spill.MemorySpillStore;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The values a spilled response hands out through {@link EsqlQueryResponse#rows()}, {@link EsqlQueryResponse#values()}
 * and {@link EsqlQueryResponse#column(int)} outlive the page they were read from, which the walk releases as it moves
 * on. Every type but one materializes an owned Java value as a matter of course; a TDigest's holder is a view into the
 * block's storage unless the extractor copies it, so that is the type exercised here.
 */
public class SpilledResponseValuesTests extends ComputeTestCase {

    public void testARetainedDigestOutlivesItsPage() throws Exception {
        MemorySpillStore store = new MemorySpillStore(
            MemorySpillStore.Completion.INLINE,
            null,
            Long.MAX_VALUE,
            "test.spill",
            new Random(randomLong())
        );
        try {
            DriverContext ctx = spillingDriverContext(ByteSizeValue.ofMb(64), 0, store, EsExecutors.DIRECT_EXECUTOR_SERVICE);
            QueryScope scope = ctx.queryScope();
            // a budget of zero sends every page to the run; the collector's lease is the result's
            ResultCollector collector = new ResultCollector(scope, scope.tryAcquire(), 0, "result");
            List<TDigestHolder> digests = new ArrayList<>();
            int pages = between(2, 6);
            int rows = between(1, 64);
            for (int p = 0; p < pages; p++) {
                try (TDigestBlockBuilder builder = ctx.blockFactory().newTDigestBlockBuilder(rows)) {
                    for (int r = 0; r < rows; r++) {
                        TDigestHolder digest = BlockTestUtils.randomTDigest();
                        digests.add(digest);
                        builder.appendTDigest(digest);
                    }
                    collector.accept(new Page(builder.build()));
                }
            }
            PlainActionFuture<ResultPages> finished = new PlainActionFuture<>();
            collector.finish(finished);
            ResultPages result = finished.get(30, TimeUnit.SECONDS);
            assertTrue("the result went to the run", result.spilled());
            EsqlQueryResponse response = new EsqlQueryResponse(
                List.of(new ColumnInfoImpl("digest", DataType.TDIGEST, null)),
                result,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                null,
                false,
                null,
                false,
                false,
                ZoneOffset.UTC,
                0,
                0,
                null,
                null
            );
            try {
                List<Object> retained = new ArrayList<>();
                switch (between(0, 2)) {
                    case 0 -> {
                        for (Iterable<Object> row : response.rows()) {
                            retained.add(row.iterator().next());
                        }
                    }
                    case 1 -> {
                        Iterator<Iterator<Object>> walk = response.values();
                        while (walk.hasNext()) {
                            retained.add(walk.next().next());
                        }
                    }
                    case 2 -> {
                        Iterator<Object> walk = response.column(0);
                        while (walk.hasNext()) {
                            retained.add(walk.next());
                        }
                    }
                    default -> throw new AssertionError();
                }
                assertThat(retained.size(), equalTo(digests.size()));
                for (int i = 0; i < digests.size(); i++) {
                    TDigestHolder expected = digests.get(i);
                    TDigestHolder got = (TDigestHolder) retained.get(i);
                    assertThat("value " + i + " is intact once its page is released", got, equalTo(expected));
                }
            } finally {
                response.decRef();
            }
        } finally {
            closeSpillingScopes();
            PlainActionFuture<Void> shutdown = new PlainActionFuture<>();
            store.shutdown(shutdown);
            shutdown.get(30, TimeUnit.SECONDS);
        }
    }

    /**
     * The extractor hands out a holder over bytes of its own, not the block's.
     */
    public void testTheExtractorCopiesTheDigest() {
        TDigestHolder digest = BlockTestUtils.randomTDigest();
        try (TDigestBlockBuilder builder = blockFactory().newTDigestBlockBuilder(1)) {
            builder.appendTDigest(digest);
            try (TDigestBlock block = builder.build()) {
                TDigestHolder view = block.getTDigestHolder(0, new TDigestHolder());
                TDigestHolder got = (TDigestHolder) ResponseValueUtils.valueExtractorFor(DataType.TDIGEST, ZoneOffset.UTC)
                    .extract(block, 0, new BytesRef());
                assertThat(got, equalTo(digest));
                assertThat(got.getEncodedDigest().bytes, not(sameInstance(view.getEncodedDigest().bytes)));
            }
        }
    }
}
