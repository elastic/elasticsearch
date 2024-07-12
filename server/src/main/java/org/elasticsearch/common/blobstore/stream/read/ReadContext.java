/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.common.blobstore.stream.read;


import org.elasticsearch.common.io.InputStreamContainer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * ReadContext is used to encapsulate all data needed by <code>BlobContainer#readBlobAsync</code>
 *
 * @opensearch.experimental
 */
public class ReadContext {
    private final long blobSize;
    private final List<StreamPartCreator> asyncPartStreams;
    private final String blobChecksum;
    private final Map<String, String> metadata;

    private ReadContext(long blobSize, List<StreamPartCreator> asyncPartStreams, String blobChecksum, Map<String, String> metadata) {
        this.blobSize = blobSize;
        this.asyncPartStreams = asyncPartStreams;
        this.blobChecksum = blobChecksum;
        this.metadata = metadata;
    }

    public ReadContext(ReadContext readContext) {
        this.blobSize = readContext.blobSize;
        this.asyncPartStreams = readContext.asyncPartStreams;
        this.blobChecksum = readContext.blobChecksum;
        this.metadata = readContext.metadata;
    }

    public String getBlobChecksum() {
        return blobChecksum;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public int getNumberOfParts() {
        return asyncPartStreams.size();
    }

    public long getBlobSize() {
        return blobSize;
    }

    public List<StreamPartCreator> getPartStreams() {
        return asyncPartStreams;
    }

    /**
     * Functional interface defining an instance that can create an async action
     * to create a part of an object represented as an InputStreamContainer.
     *
     * @opensearch.experimental
     */
    @FunctionalInterface
    public interface StreamPartCreator extends Supplier<CompletableFuture<InputStreamContainer>> {
        /**
         * Kicks off an async process to start streaming.
         *
         * @return When the returned future is completed, streaming has
         * just begun. Clients must fully consume the resulting stream.
         */
        @Override
        CompletableFuture<InputStreamContainer> get();
    }

    /**
     * Builder for {@link ReadContext}.
     *
     * @opensearch.experimental
     */
    public static class Builder {
        private final long blobSize;
        private final List<StreamPartCreator> asyncPartStreams;
        private String blobChecksum;
        private Map<String, String> metadata;

        public Builder(long blobSize, List<StreamPartCreator> asyncPartStreams) {
            this.blobSize = blobSize;
            this.asyncPartStreams = asyncPartStreams;
        }

        public Builder blobChecksum(String blobChecksum) {
            this.blobChecksum = blobChecksum;
            return this;
        }

        public Builder metadata(Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public ReadContext build() {
            return new ReadContext(blobSize, asyncPartStreams, blobChecksum, metadata);
        }

    }
}
