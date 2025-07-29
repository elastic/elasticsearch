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
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Function;

public class CollectOperator implements Operator {
    public interface Writer {
        XContentBuilder write(XContentBuilder builder, int valueIndex) throws IOException;
    }

    public record Factory(Client client, String index, List<Function<Block, Writer>> writers) implements OperatorFactory {
        @Override
        public CollectOperator get(DriverContext driverContext) {
            return new CollectOperator(client, driverContext, index, writers);
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
    private final List<Function<Block, Writer>> writers;

    private volatile Phase phase = Phase.COLLECTING;
    private volatile IsBlockedResult blocked = NOT_BLOCKED;

    private int pagesReceived;
    private int pagesEmitted;
    private long rowsReceived;
    private long rowsEmitted;
    private long bulkBytesSent;

    public CollectOperator(Client client, DriverContext driverContext, String index, List<Function<Block, Writer>> writers) {
        this.client = client;
        this.driverContext = driverContext;
        this.index = index;
        this.writers = writers;
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
            bulkBytesSent += request.estimatedSizeInBytes();
            Listener listener = new Listener(page.getPositionCount());
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
            Writer writer = writers.get(b).apply(block);
            for (int p = 0; p < block.getPositionCount(); p++) {
                writer.write(source[p], p);
            }
        }
        BulkRequest request = new BulkRequest();
        for (int p = 0; p < page.getPositionCount(); p++) {
            request.add(new IndexRequest(index).source(source[p].endObject()));
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
        Block rowCount = null;
        try {
            rowCount = driverContext.blockFactory().newConstantLongBlockWith(rowsEmitted, 1);
            Page result = new Page(rowCount);
            rowCount = null;
            phase = Phase.FINISHED;
            return result;
        } finally {
            Releasables.close(rowCount);
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

        Listener(int positionCount) {
            driverContext.addAsyncAction();
            this.positionCount = positionCount;
        }

        @Override
        public void onResponse(BulkResponse bulkItemResponses) {
            pagesEmitted++;
            rowsEmitted += positionCount;
            if (bulkItemResponses.hasFailures()) {
                failureCollector.unwrapAndCollect(new ElasticsearchException(bulkItemResponses.buildFailureMessage()));
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
