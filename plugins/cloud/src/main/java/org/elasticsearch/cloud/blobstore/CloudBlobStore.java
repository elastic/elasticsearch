/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.cloud.blobstore;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.blobstore.AppendableBlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.ImmutableBlobContainer;
import org.elasticsearch.common.blobstore.support.ImmutableAppendableBlobContainer;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.jclouds.blobstore.AsyncBlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.domain.Location;

import java.util.Set;
import java.util.concurrent.Executor;

/**
 * @author kimchy (shay.banon)
 */
public class CloudBlobStore extends AbstractComponent implements BlobStore {

    private final BlobStoreContext blobStoreContext;

    private final String container;

    private final Location location;

    private final Executor executor;

    private final int bufferSizeInBytes;

    public CloudBlobStore(Settings settings, BlobStoreContext blobStoreContext, Executor executor, String container, String location) {
        super(settings);
        this.blobStoreContext = blobStoreContext;
        this.container = container;
        this.executor = executor;

        this.bufferSizeInBytes = (int) settings.getAsBytesSize("buffer_size", new ByteSizeValue(100, ByteSizeUnit.KB)).bytes();

        if (location == null) {
            this.location = null;
        } else {
            Location matchedLocation = null;
            Set<? extends Location> assignableLocations = blobStoreContext.getBlobStore().listAssignableLocations();
            for (Location oLocation : assignableLocations) {
                if (oLocation.getId().equals(location)) {
                    matchedLocation = oLocation;
                    break;
                }
            }
            this.location = matchedLocation;
            if (this.location == null) {
                throw new ElasticSearchIllegalArgumentException("Not a valid location [" + location + "], available locations " + assignableLocations);
            }
        }
        logger.debug("Using location [{}], container [{}]", this.location, this.container);
        sync().createContainerInLocation(this.location, container);
    }

    @Override public String toString() {
        return container;
    }

    public int bufferSizeInBytes() {
        return this.bufferSizeInBytes;
    }

    public Executor executor() {
        return executor;
    }

    public String container() {
        return this.container;
    }

    public Location location() {
        return this.location;
    }

    public AsyncBlobStore async() {
        return blobStoreContext.getAsyncBlobStore();
    }

    public org.jclouds.blobstore.BlobStore sync() {
        return blobStoreContext.getBlobStore();
    }

    @Override public ImmutableBlobContainer immutableBlobContainer(BlobPath path) {
        return new CloudImmutableBlobContainer(path, this);
    }

    @Override public AppendableBlobContainer appendableBlobContainer(BlobPath path) {
        return new ImmutableAppendableBlobContainer(immutableBlobContainer(path));
    }

    @Override public void delete(BlobPath path) {
        sync().deleteDirectory(container, path.buildAsString("/"));
    }

    @Override public void close() {
    }
}
