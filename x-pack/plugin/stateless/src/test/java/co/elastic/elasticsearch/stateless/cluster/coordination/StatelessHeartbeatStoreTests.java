/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.cluster.coordination;

import co.elastic.elasticsearch.stateless.test.FakeStatelessNode;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.coordination.stateless.Heartbeat;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class StatelessHeartbeatStoreTests extends ESTestCase {
    public void testStoresHeartbeatIntoTheBlobStore() throws Exception {
        try (var statelessNode = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry())) {
            var objectStoreService = statelessNode.objectStoreService;
            var heartbeatStore = new StatelessHeartbeatStore(objectStoreService::getLeaderHeartbeatContainer, statelessNode.threadPool);

            var heartbeat = randomHeartbeat();
            PlainActionFuture.<Void, Exception>get(f -> heartbeatStore.writeHeartbeat(heartbeat, f));
            var readHeartbeat = PlainActionFuture.get(heartbeatStore::readLatestHeartbeat);
            assertThat(heartbeat, equalTo(readHeartbeat));
        }
    }

    public void testStoreHeartbeatUnderFailure() throws Exception {
        try (var statelessNode = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry()) {
            @Override
            public BlobContainer wrapBlobContainer(BlobPath path, BlobContainer innerContainer) {
                return new FilterBlobContainer(super.wrapBlobContainer(path, innerContainer)) {
                    @Override
                    protected BlobContainer wrapChild(BlobContainer child) {
                        return child;
                    }

                    @Override
                    public void writeMetadataBlob(
                        String blobName,
                        boolean failIfAlreadyExists,
                        boolean atomic,
                        CheckedConsumer<OutputStream, IOException> writer
                    ) throws IOException {
                        throw new IOException("Unable to read " + blobName);
                    }
                };
            }
        }) {
            var objectStoreService = statelessNode.objectStoreService;
            var heartbeatStore = new StatelessHeartbeatStore(objectStoreService::getLeaderHeartbeatContainer, statelessNode.threadPool);

            var heartbeat = randomHeartbeat();
            expectThrows(Exception.class, () -> PlainActionFuture.<Void, Exception>get(f -> heartbeatStore.writeHeartbeat(heartbeat, f)));

            var readHeartbeat = PlainActionFuture.get(heartbeatStore::readLatestHeartbeat);
            assertThat(readHeartbeat, is(nullValue()));
        }
    }

    public void testVerifiesChecksumDuringReads() throws Exception {
        try (var statelessNode = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry()) {
            @Override
            public BlobContainer wrapBlobContainer(BlobPath path, BlobContainer innerContainer) {
                return new FilterBlobContainer(super.wrapBlobContainer(path, innerContainer)) {
                    @Override
                    protected BlobContainer wrapChild(BlobContainer child) {
                        return child;
                    }

                    @Override
                    public void writeMetadataBlob(
                        String blobName,
                        boolean failIfAlreadyExists,
                        boolean atomic,
                        CheckedConsumer<OutputStream, IOException> writer
                    ) throws IOException {
                        super.writeMetadataBlob(blobName, failIfAlreadyExists, atomic, (out) -> {
                            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                                writer.accept(outputStream);
                                byte[] data = outputStream.toByteArray();

                                // Flip one byte somewhere
                                int i = randomIntBetween(0, data.length - 1);
                                data[i] = (byte) ~data[i];
                                out.write(data);
                            }
                        });
                    }
                };
            }
        }) {
            var objectStoreService = statelessNode.objectStoreService;
            var heartbeatStore = new StatelessHeartbeatStore(objectStoreService::getLeaderHeartbeatContainer, statelessNode.threadPool);

            var heartbeat = randomHeartbeat();
            PlainActionFuture.<Void, Exception>get(f -> heartbeatStore.writeHeartbeat(heartbeat, f));

            assertThat(
                expectThrows(IllegalStateException.class, () -> PlainActionFuture.get(heartbeatStore::readLatestHeartbeat)).getMessage(),
                containsString("checksum verification failed")
            );
        }
    }

    private Heartbeat randomHeartbeat() {
        return new Heartbeat(randomLongBetween(0, Long.MAX_VALUE), randomLongBetween(0, Long.MAX_VALUE));
    }

}
