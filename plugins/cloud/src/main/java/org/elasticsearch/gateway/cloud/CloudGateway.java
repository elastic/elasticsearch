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

package org.elasticsearch.gateway.cloud;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.cloud.blobstore.CloudBlobStoreService;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.gateway.GatewayException;
import org.elasticsearch.index.gateway.cloud.CloudIndexGatewayModule;
import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.component.AbstractLifecycleComponent;
import org.elasticsearch.util.inject.Inject;
import org.elasticsearch.util.inject.Module;
import org.elasticsearch.util.io.FastByteArrayInputStream;
import org.elasticsearch.util.settings.Settings;
import org.elasticsearch.util.xcontent.ToXContent;
import org.elasticsearch.util.xcontent.XContentFactory;
import org.elasticsearch.util.xcontent.XContentParser;
import org.elasticsearch.util.xcontent.XContentType;
import org.elasticsearch.util.xcontent.builder.BinaryXContentBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.domain.Location;

import java.io.IOException;
import java.util.Set;

import static org.jclouds.blobstore.options.ListContainerOptions.Builder.*;

/**
 * @author kimchy (shay.banon)
 */
public class CloudGateway extends AbstractLifecycleComponent<Gateway> implements Gateway {

    private final BlobStoreContext blobStoreContext;


    private final String container;

    private final Location location;

    private final String metaDataDirectory;

    private final SizeValue chunkSize;

    private volatile int currentIndex;

    @Inject public CloudGateway(Settings settings, ClusterName clusterName, CloudBlobStoreService blobStoreService) {
        super(settings);
        this.blobStoreContext = blobStoreService.context();

        this.chunkSize = componentSettings.getAsSize("chunk_size", null);

        String location = componentSettings.get("location");
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

        this.container = componentSettings.get("container");
        if (container == null) {
            throw new ElasticSearchIllegalArgumentException("Cloud gateway requires 'container' setting");
        }
        this.metaDataDirectory = clusterName.value() + "/metadata";
        logger.debug("Using location [{}], container [{}], metadata_directory [{}]", this.location, this.container, metaDataDirectory);
        blobStoreContext.getBlobStore().createContainerInLocation(this.location, container);

        this.currentIndex = findLatestIndex();
        logger.debug("Latest metadata found at index [" + currentIndex + "]");
    }

    public String container() {
        return this.container;
    }

    public Location location() {
        return this.location;
    }

    public SizeValue chunkSize() {
        return this.chunkSize;
    }

    @Override protected void doStart() throws ElasticSearchException {
    }

    @Override protected void doStop() throws ElasticSearchException {
    }

    @Override protected void doClose() throws ElasticSearchException {
    }

    @Override public void write(MetaData metaData) throws GatewayException {
        try {
            String name = metaDataDirectory + "/metadata-" + (currentIndex + 1);

            BinaryXContentBuilder builder = XContentFactory.contentBinaryBuilder(XContentType.JSON);
            builder.prettyPrint();
            builder.startObject();
            MetaData.Builder.toXContent(metaData, builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();

            Blob blob = blobStoreContext.getBlobStore().newBlob(name);
            blob.setPayload(new FastByteArrayInputStream(builder.unsafeBytes(), 0, builder.unsafeBytesLength()));
            blob.setContentLength(builder.unsafeBytesLength());

            blobStoreContext.getBlobStore().putBlob(container, blob);

            currentIndex++;

            PageSet<? extends StorageMetadata> pageSet = blobStoreContext.getBlobStore().list(container, inDirectory(metaDataDirectory));
            for (StorageMetadata storageMetadata : pageSet) {
                if (storageMetadata.getName().contains("metadata-") && !name.equals(storageMetadata.getName())) {
                    blobStoreContext.getAsyncBlobStore().removeBlob(container, storageMetadata.getName());
                }
            }
        } catch (IOException e) {
            throw new GatewayException("can't write new metadata file into the gateway", e);
        }
    }

    @Override public MetaData read() throws GatewayException {
        try {
            if (currentIndex == -1)
                return null;

            return readMetaData(metaDataDirectory + "/metadata-" + currentIndex);
        } catch (GatewayException e) {
            throw e;
        } catch (Exception e) {
            throw new GatewayException("can't read metadata file from the gateway", e);
        }
    }

    @Override public Class<? extends Module> suggestIndexGateway() {
        return CloudIndexGatewayModule.class;
    }

    @Override public void reset() {
        PageSet<? extends StorageMetadata> pageSet = blobStoreContext.getBlobStore().list(container, inDirectory(metaDataDirectory));
        for (StorageMetadata storageMetadata : pageSet) {
            if (storageMetadata.getName().contains("metadata-")) {
                blobStoreContext.getBlobStore().removeBlob(container, storageMetadata.getName());
            }
        }
        currentIndex = -1;
    }

    private int findLatestIndex() {
        int index = -1;
        PageSet<? extends StorageMetadata> pageSet = blobStoreContext.getBlobStore().list(container, inDirectory(metaDataDirectory).maxResults(1000));
        for (StorageMetadata storageMetadata : pageSet) {
            if (logger.isTraceEnabled()) {
                logger.trace("[findLatestMetadata]: Processing blob [" + storageMetadata.getName() + "]");
            }
            if (!storageMetadata.getName().contains("metadata-")) {
                continue;
            }
            int fileIndex = Integer.parseInt(storageMetadata.getName().substring(storageMetadata.getName().lastIndexOf('-') + 1));
            if (fileIndex >= index) {
                // try and read the meta data
                try {
                    readMetaData(storageMetadata.getName());
                    index = fileIndex;
                } catch (IOException e) {
                    logger.warn("[findLatestMetadata]: Failed to read metadata from [" + storageMetadata.getName() + "], ignoring...", e);
                }
            }
        }
        return index;
    }

    private MetaData readMetaData(String name) throws IOException {
        XContentParser parser = null;
        try {
            Blob blob = blobStoreContext.getBlobStore().getBlob(container, name);
            parser = XContentFactory.xContent(XContentType.JSON).createParser(blob.getContent());
            return MetaData.Builder.fromXContent(parser, settings);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }
}

