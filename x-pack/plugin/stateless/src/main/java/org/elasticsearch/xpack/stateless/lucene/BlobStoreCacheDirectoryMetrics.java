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

package org.elasticsearch.xpack.stateless.lucene;

import org.elasticsearch.index.store.DirectoryMetrics;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.function.Supplier;

public class BlobStoreCacheDirectoryMetrics implements DirectoryMetrics.PluggableMetrics<BlobStoreCacheDirectoryMetrics> {
    private long waitTime;
    private long waits;
    private long waitBytes;

    public BlobStoreCacheDirectoryMetrics() {}

    private BlobStoreCacheDirectoryMetrics(long waitTime, long waits, long waitBytes) {
        this.waitTime = waitTime;
        this.waits = waits;
        this.waitBytes = waitBytes;
    }

    @Override
    public BlobStoreCacheDirectoryMetrics copy() {
        return new BlobStoreCacheDirectoryMetrics(waitTime, waits, waitBytes);
    }

    @Override
    public Supplier<BlobStoreCacheDirectoryMetrics> delta() {
        BlobStoreCacheDirectoryMetrics snapshot = copy();
        return () -> copy().minus(snapshot);
    }

    private BlobStoreCacheDirectoryMetrics minus(BlobStoreCacheDirectoryMetrics snapshot) {
        return new BlobStoreCacheDirectoryMetrics(waitTime - snapshot.waitTime, waits - snapshot.waits, waitBytes - snapshot.waitBytes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("wait_time", waitTime);
        builder.field("waits", waits);
        builder.field("wait_bytes", waitBytes);
        return builder;
    }

    public void add(long time, long bytes) {
        ++waits;
        waitTime += time;
        waitBytes += bytes;
    }

    public long getWaitTime() {
        return waitTime;
    }

    public long getWaits() {
        return waits;
    }

    public long getWaitBytes() {
        return waitBytes;
    }
}
