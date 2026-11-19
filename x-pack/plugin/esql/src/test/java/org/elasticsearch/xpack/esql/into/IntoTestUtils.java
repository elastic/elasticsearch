/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.EscfBulkService;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Shared test infrastructure for {@code INTO} operator tests.
 */
class IntoTestUtils {
    static final String DEST = "dst";
    static final ShardId SHARD = new ShardId(DEST, "uuid", 0);

    enum Outcome {
        CREATED,
        REFUSED
    }

    static class CapturingClient extends NoOpClient {
        final List<BulkRequest> bulks = new ArrayList<>();
        final List<ActionListener<BulkResponse>> listeners = new ArrayList<>();

        CapturingClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            assertThat(action, equalTo(TransportBulkAction.TYPE));
            bulks.add((BulkRequest) request);
            listeners.add((ActionListener<BulkResponse>) listener);
        }

        void answer(Outcome... outcomes) {
            BulkRequest bulk = bulks.removeFirst();
            assertThat(bulk.numberOfActions(), equalTo(outcomes.length));
            BulkItemResponse[] items = new BulkItemResponse[outcomes.length];
            for (int i = 0; i < outcomes.length; i++) {
                DocWriteRequest.OpType opType = bulk.requests().get(i).opType();
                items[i] = switch (outcomes[i]) {
                    case CREATED -> BulkItemResponse.success(i, opType, new IndexResponse(SHARD, "id-" + i, 0, 1, 1, true));
                    case REFUSED -> BulkItemResponse.failure(
                        i,
                        opType,
                        new BulkItemResponse.Failure(DEST, "id-" + i, new IllegalArgumentException("mapper_parsing_exception"))
                    );
                };
            }
            listeners.removeFirst().onResponse(new BulkResponse(items, 1));
        }

        void fail(Exception e) {
            bulks.removeFirst();
            listeners.removeFirst().onFailure(e);
        }
    }

    /** An {@link EscfBulkService} whose callbacks run on the calling thread, so {@code answer()} settles a writer before it returns. */
    static EscfBulkService bulks() {
        IndexingPressure pressure = new IndexingPressure(
            Settings.builder().put(IndexingPressure.MAX_COORDINATING_BYTES.getKey(), "1gb").build()
        );
        return new EscfBulkService(pressure, Runnable::run, mockBigArrays().bytesRefRecycler());
    }

    static BigArrays mockBigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), ByteSizeValue.ofGb(1));
    }

    static IntoStats stats(Page page) {
        try {
            assertThat(page.getPositionCount(), equalTo(1));
            LongBlock total = page.getBlock(0);
            LongBlock created = page.getBlock(1);
            return new IntoStats(total.getLong(0), created.getLong(0));
        } finally {
            page.releaseBlocks();
        }
    }
}
