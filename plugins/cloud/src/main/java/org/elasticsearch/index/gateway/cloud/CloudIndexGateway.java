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

package org.elasticsearch.index.gateway.cloud;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.cloud.blobstore.CloudBlobStoreService;
import org.elasticsearch.cloud.jclouds.JCloudsUtils;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.gateway.cloud.CloudGateway;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.gateway.IndexGateway;
import org.elasticsearch.index.gateway.IndexShardGateway;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.util.SizeUnit;
import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.inject.Inject;
import org.elasticsearch.util.settings.Settings;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.domain.Location;

import java.util.Set;

/**
 * @author kimchy (shay.banon)
 */
public class CloudIndexGateway extends AbstractIndexComponent implements IndexGateway {

    private final Gateway gateway;

    private final String indexContainer;

    private final Location location;

    private final SizeValue chunkSize;

    private final BlobStoreContext blobStoreContext;

    @Inject public CloudIndexGateway(Index index, @IndexSettings Settings indexSettings, CloudBlobStoreService blobStoreService, Gateway gateway) {
        super(index, indexSettings);
        this.blobStoreContext = blobStoreService.context();
        this.gateway = gateway;

        String location = componentSettings.get("location");
        String container = componentSettings.get("container");
        SizeValue chunkSize = componentSettings.getAsSize("chunk_size", null);

        if (gateway instanceof CloudGateway) {
            CloudGateway cloudGateway = (CloudGateway) gateway;
            if (container == null) {
                container = cloudGateway.container() + JCloudsUtils.BLOB_CONTAINER_SEP + index.name();
            }
            if (chunkSize == null) {
                chunkSize = cloudGateway.chunkSize();
            }
        }

        if (chunkSize == null) {
            chunkSize = new SizeValue(4, SizeUnit.GB);
        }

        if (location == null) {
            if (gateway instanceof CloudGateway) {
                CloudGateway cloudGateway = (CloudGateway) gateway;
                this.location = cloudGateway.location();
            } else {
                this.location = null;
            }
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
        this.indexContainer = container;
        this.chunkSize = chunkSize;

        logger.debug("Using location [{}], container [{}], chunk_size [{}]", this.location, this.indexContainer, this.chunkSize);

//        blobStoreContext.getBlobStore().createContainerInLocation(this.location, this.indexContainer);
    }

    public Location indexLocation() {
        return this.location;
    }

    public String indexContainer() {
        return this.indexContainer;
    }

    public SizeValue chunkSize() {
        return this.chunkSize;
    }

    @Override public Class<? extends IndexShardGateway> shardGatewayClass() {
        return CloudIndexShardGateway.class;
    }

    @Override public void close(boolean delete) throws ElasticSearchException {
        if (!delete) {
            return;
        }
//        blobStoreContext.getBlobStore().deleteContainer(indexContainer);
    }
}
