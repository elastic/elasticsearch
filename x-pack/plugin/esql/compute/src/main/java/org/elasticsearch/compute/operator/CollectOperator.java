/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.List;
import java.util.function.Function;

public class CollectOperator implements Operator {
    public static final long ID_SEED = 0;

    public interface FieldWriter {
        XContentBuilder write(XContentBuilder builder, int position) throws IOException;
    }

    public interface IdWriter {
        void write(BreakingBytesRefBuilder builder, int position);
    }

    public record Factory(
        Client client,
        String index,
        List<Function<Block, FieldWriter>> fieldWriters,
        List<Function<Page, IdWriter>> idWriters
    ) implements OperatorFactory {
        @Override
        public CollectOperator get(DriverContext driverContext) {
            return new CollectOperator(client, driverContext, index, fieldWriters, idWriters);
        }

        @Override
        public String describe() {
            return "CollectOperator";
        }
    }

    private final FailureCollector failureCollector = new FailureCollector();

    private final Client client;
    private final DriverContext driverContext;
    private final String index;
    private final List<Function<Block, FieldWriter>> fieldWriters;
    private final List<Function<Page, IdWriter>> idWriters;
    private final BreakingBytesRefBuilder idBuilder;

    private volatile Phase phase = Phase.COLLECTING;
    private volatile IsBlockedResult blocked = NOT_BLOCKED;

    private int pagesReceived;
    private int pagesSaved;
    private long rowsReceived;
    private long rowsSaved;
    private long rowsCreated;
    private long rowsUpdated;
    private long rowsNoop;
    private long bulkBytesSent;
    private long bulkBytesSaved;
    private long bulkTookMillis;
    private long bulkIngestTookMillis;

    public CollectOperator(
        Client client,
        DriverContext driverContext,
        String index,
        List<Function<Block, FieldWriter>> fieldWriters,
        List<Function<Page, IdWriter>> idWriters
    ) {
        this.client = client;
        this.driverContext = driverContext;
        this.index = index;
        this.fieldWriters = fieldWriters;
        this.idWriters = idWriters;
        this.idBuilder = new BreakingBytesRefBuilder(driverContext.breaker(), "id");
    }

    @Override
    public boolean needsInput() {
        return failureCollector.hasFailure() == false && phase == Phase.COLLECTING && blocked.listener().isDone();
    }

    @Override
    public void addInput(Page page) {
        assert needsInput();
        checkFailure();
        pagesReceived++;
        rowsReceived += page.getPositionCount();
        try {
            BulkRequest request = request(page);
            long bulkSize = request.estimatedSizeInBytes();
            bulkBytesSent += bulkSize;
            Listener listener = new Listener(page.getPositionCount(), bulkSize);
            blocked = new IsBlockedResult(listener.blockedFuture, "indexing");
            client.bulk(request, listener);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private BulkRequest request(Page page) throws IOException {
        XContentBuilder[] source = new XContentBuilder[page.getPositionCount()];
        for (int p = 0; p < page.getPositionCount(); p++) {
            source[p] = JsonXContent.contentBuilder().startObject();
        }
        for (int b = 0; b < page.getBlockCount(); b++) {
            Block block = page.getBlock(b);
            FieldWriter writer = fieldWriters.get(b).apply(block);
            for (int p = 0; p < block.getPositionCount(); p++) {
                writer.write(source[p], p);
            }
        }
        List<IdWriter> idWriters = this.idWriters.stream().map(w -> w.apply(page)).toList();
        BulkRequest request = new BulkRequest();
        MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
        byte[] hashBytes = hash.getBytes();
        for (int p = 0; p < page.getPositionCount(); p++) {
            source[p].endObject();
            if (idWriters.isEmpty()) {
                request.add(new IndexRequest(index).source(source[p]));
            } else {
                for (IdWriter idWriter : idWriters) {
                    idWriter.write(idBuilder, p);
                }
                MurmurHash3.hash128(idBuilder.bytes(), 0, idBuilder.length(), ID_SEED, hash);
                hash.getBytes(hashBytes, 0);
                String id = Base64.getUrlEncoder().encodeToString(hashBytes);
                request.add(new UpdateRequest().index(index).id(id).doc(source[p]).docAsUpsert(true));
                idBuilder.clear();
            }
        }
        return request;
    }

    @Override
    public void finish() {
        if (phase != Phase.COLLECTING) {
            return;
        }
        phase = Phase.WAITING_TO_FINISH;
        checkFailure();
        blocked.listener().addListener(new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                phase = Phase.READY_TO_OUTPUT;
            }

            @Override
            public void onFailure(Exception e) {
                failureCollector.unwrapAndCollect(e);
            }
        });
    }

    @Override
    public boolean isFinished() {
        return phase == Phase.FINISHED;
    }

    @Override
    public IsBlockedResult isBlocked() {
        return blocked;
    }

    @Override
    public Page getOutput() {
        checkFailure();
        if (phase != Phase.READY_TO_OUTPUT) {
            return null;
        }
        Block rowsSaved = null;
        Block rowsCreated = null;
        Block rowsUpdated = null;
        Block rowsNoop = null;
        Block bytesSaved = null;
        Block bulkTookMillis = null;
        Block bulkIngestTookMillis = null;
        try {
            rowsSaved = driverContext.blockFactory().newConstantLongBlockWith(this.rowsSaved, 1);
            rowsCreated = driverContext.blockFactory().newConstantLongBlockWith(this.rowsCreated, 1);
            rowsUpdated = driverContext.blockFactory().newConstantLongBlockWith(this.rowsUpdated, 1);
            rowsNoop = driverContext.blockFactory().newConstantLongBlockWith(this.rowsNoop, 1);
            bytesSaved = driverContext.blockFactory().newConstantLongBlockWith(this.bulkBytesSaved, 1);
            bulkTookMillis = driverContext.blockFactory().newConstantLongBlockWith(this.bulkTookMillis, 1);
            bulkIngestTookMillis = driverContext.blockFactory().newConstantLongBlockWith(this.bulkIngestTookMillis, 1);
            Page result = new Page(rowsSaved, rowsCreated, rowsUpdated, rowsNoop, bytesSaved, bulkTookMillis, bulkIngestTookMillis);
            rowsSaved = null;
            rowsCreated = null;
            rowsUpdated = null;
            rowsNoop = null;
            bytesSaved = null;
            bulkTookMillis = null;
            bulkIngestTookMillis = null;
            phase = Phase.FINISHED;
            return result;
        } finally {
            Releasables.close(rowsSaved, rowsCreated, rowsUpdated, rowsNoop, bytesSaved, bulkTookMillis, bulkIngestTookMillis);
        }
    }

    @Override
    public void close() {}

    private void checkFailure() {
        Exception e = failureCollector.getFailure();
        if (e != null) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    private enum Phase {
        COLLECTING,
        WAITING_TO_FINISH,
        READY_TO_OUTPUT,
        FINISHED;
    }

    private class Listener implements ActionListener<BulkResponse> {
        private final SubscribableListener<Void> blockedFuture = new SubscribableListener<>();
        private final int positionCount;
        private final long bulkSize;

        Listener(int positionCount, long bulkSize) {
            driverContext.addAsyncAction();
            this.positionCount = positionCount;
            this.bulkSize = bulkSize;
        }

        @Override
        public void onResponse(BulkResponse bulkItemResponses) {
            if (bulkItemResponses.hasFailures()) {
                onFailure(new ElasticsearchException(bulkItemResponses.buildFailureMessage()));
                return;
            }
            pagesSaved++;
            rowsSaved += positionCount;
            for (BulkItemResponse i : bulkItemResponses.getItems()) {
                switch (i.getResponse().getResult()) {
                    case CREATED -> rowsCreated++;
                    case UPDATED -> rowsUpdated++;
                    case NOOP -> rowsNoop++;
                    case DELETED, NOT_FOUND -> {
                        assert false : "delete and not_found not supported but was [" + i.getResponse().getResult() + "]";
                    }
                }
            }
            bulkBytesSaved += bulkSize;
            bulkTookMillis += bulkItemResponses.getTookInMillis();
            if (bulkItemResponses.getIngestTookInMillis() != BulkResponse.NO_INGEST_TOOK) {
                bulkIngestTookMillis += bulkItemResponses.getIngestTookInMillis();
            }
            unblock();
        }

        @Override
        public void onFailure(Exception e) {
            failureCollector.unwrapAndCollect(e);
            unblock();
        }

        private void unblock() {
            driverContext.removeAsyncAction();
            blockedFuture.onResponse(null);
        }
    }
}
