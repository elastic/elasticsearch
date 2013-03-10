/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.gateway.blobstore;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.gateway.blobstore.BlobStoreGateway;
import org.elasticsearch.gateway.none.NoneGateway;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.gateway.IndexGateway;
import org.elasticsearch.index.settings.IndexSettings;

/**
 *
 */
public abstract class BlobStoreIndexGateway extends AbstractIndexComponent implements IndexGateway {

    private final BlobStoreGateway gateway;

    private final BlobStore blobStore;

    private final BlobPath indexPath;

    protected ByteSizeValue chunkSize;

    protected BlobStoreIndexGateway(Index index, @IndexSettings Settings indexSettings, Gateway gateway) {
        super(index, indexSettings);

        if (gateway.type().equals(NoneGateway.TYPE)) {
            logger.warn("index gateway is configured, but no cluster level gateway configured, cluster level metadata will be lost on full shutdown");
        }

        this.gateway = (BlobStoreGateway) gateway;
        this.blobStore = this.gateway.blobStore();

        this.chunkSize = componentSettings.getAsBytesSize("chunk_size", this.gateway.chunkSize());

        this.indexPath = this.gateway.basePath().add("indices").add(index.name());
    }

    @Override
    public String toString() {
        return type() + "://" + blobStore + "/" + indexPath;
    }

    public BlobStore blobStore() {
        return blobStore;
    }

    public ByteSizeValue chunkSize() {
        return this.chunkSize;
    }

    public BlobPath shardPath(int shardId) {
        return indexPath.add(Integer.toString(shardId));
    }

    public static BlobPath shardPath(BlobPath basePath, String index, int shardId) {
        return basePath.add("indices").add(index).add(Integer.toString(shardId));
    }

    @Override
    public void close() throws ElasticSearchException {
    }
}
