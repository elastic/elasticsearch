/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.cluster.coordination;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.cluster.coordination.stateless.Heartbeat;
import org.elasticsearch.cluster.coordination.stateless.HeartbeatStore;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.index.translog.BufferedChecksumStreamInput;
import org.elasticsearch.index.translog.BufferedChecksumStreamOutput;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.NoSuchFileException;
import java.util.function.Supplier;

public class StatelessHeartbeatStore implements HeartbeatStore {
    public static final String HEARTBEAT_BLOB = "heartbeat";
    private final Supplier<BlobContainer> heartbeatBlobContainerSupplier;
    private final ThreadPool threadPool;

    public StatelessHeartbeatStore(Supplier<BlobContainer> heartbeatBlobContainerSupplier, ThreadPool threadPool) {
        this.heartbeatBlobContainerSupplier = heartbeatBlobContainerSupplier;
        this.threadPool = threadPool;
    }

    @Override
    public void writeHeartbeat(Heartbeat newHeartbeat, ActionListener<Void> listener) {
        threadPool.executor(getExecutor())
            .execute(
                ActionRunnable.run(
                    listener,
                    () -> getHeartbeatBlobContainer().writeMetadataBlob(
                        OperationPurpose.CLUSTER_STATE,
                        HEARTBEAT_BLOB,
                        false,
                        true,
                        out -> serialize(newHeartbeat, out)
                    )
                )
            );
    }

    @Override
    public void readLatestHeartbeat(ActionListener<Heartbeat> listener) {
        threadPool.executor(getExecutor()).execute(ActionRunnable.supply(listener, () -> {
            try (InputStream inputStream = getHeartbeatBlobContainer().readBlob(OperationPurpose.CLUSTER_STATE, HEARTBEAT_BLOB)) {
                return deserialize(inputStream);
            } catch (NoSuchFileException e) {
                return null;
            }
        }));
    }

    private BlobContainer getHeartbeatBlobContainer() {
        return heartbeatBlobContainerSupplier.get();
    }

    protected String getExecutor() {
        return ThreadPool.Names.SNAPSHOT_META;
    }

    private void serialize(Heartbeat heartbeat, OutputStream output) throws IOException {
        BufferedChecksumStreamOutput checksumStreamOutput = new BufferedChecksumStreamOutput(new OutputStreamStreamOutput(output));
        heartbeat.writeTo(checksumStreamOutput);
        checksumStreamOutput.writeInt((int) checksumStreamOutput.getChecksum());
        checksumStreamOutput.flush();
    }

    private Heartbeat deserialize(InputStream inputStream) throws IOException {
        try (BufferedChecksumStreamInput in = new BufferedChecksumStreamInput(new InputStreamStreamInput(inputStream), HEARTBEAT_BLOB)) {
            var heartbeat = new Heartbeat(in);

            long expectedChecksum = in.getChecksum();
            long readChecksum = Integer.toUnsignedLong(in.readInt());
            if (readChecksum != expectedChecksum) {
                throw new IllegalStateException(
                    "checksum verification failed - expected: 0x"
                        + Long.toHexString(expectedChecksum)
                        + ", got: 0x"
                        + Long.toHexString(readChecksum)
                );
            }

            return heartbeat;
        }
    }
}
