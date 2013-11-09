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

package org.elasticsearch.repositories.uri;

import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.url.URLBlobStore;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryName;
import org.elasticsearch.repositories.RepositorySettings;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Read-only URL-based implementation of the BlobStoreRepository
 * <p/>
 * This repository supports the following settings
 * <dl>
 * <dt>{@code url}</dt><dd>URL to the root of repository. This is mandatory parameter.</dd>
 * <dt>{@code concurrent_streams}</dt><dd>Number of concurrent read/write stream (per repository on each node). Defaults to 5.</dd>
 * </ol>
 */
public class URLRepository extends BlobStoreRepository {

    public final static String TYPE = "url";

    private final URLBlobStore blobStore;

    /**
     * Constructs new read-only URL-based repository
     *
     * @param name                 repository name
     * @param repositorySettings   repository settings
     * @param indexShardRepository shard repository
     * @throws IOException
     */
    @Inject
    public URLRepository(RepositoryName name, RepositorySettings repositorySettings, IndexShardRepository indexShardRepository) throws IOException {
        super(name.getName(), repositorySettings, indexShardRepository);
        URL url;
        String path = repositorySettings.settings().get("url", componentSettings.get("url"));
        if (path == null) {
            throw new RepositoryException(name.name(), "missing url");
        } else {
            url = new URL(path);
        }
        int concurrentStreams = repositorySettings.settings().getAsInt("concurrent_streams", componentSettings.getAsInt("concurrent_streams", 5));
        ExecutorService concurrentStreamPool = EsExecutors.newScaling(1, concurrentStreams, 60, TimeUnit.SECONDS, EsExecutors.daemonThreadFactory(settings, "[fs_stream]"));
        blobStore = new URLBlobStore(componentSettings, concurrentStreamPool, url);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected BlobStore blobStore() {
        return blobStore;
    }
}
