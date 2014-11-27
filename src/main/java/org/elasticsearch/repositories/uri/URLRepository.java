/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableList;
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.url.URLBlobStore;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryName;
import org.elasticsearch.repositories.RepositorySettings;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.net.URL;

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

    private final BlobPath basePath;

    private boolean listDirectories;

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
        listDirectories = repositorySettings.settings().getAsBoolean("list_directories", componentSettings.getAsBoolean("list_directories", true));
        blobStore = new URLBlobStore(componentSettings, url);
        basePath = BlobPath.cleanPath();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected BlobStore blobStore() {
        return blobStore;
    }

    @Override
    protected BlobPath basePath() {
        return basePath;
    }

    @Override
    public ImmutableList<SnapshotId> snapshots() {
        if (listDirectories) {
            return super.snapshots();
        } else {
            try {
                return readSnapshotList();
            } catch (IOException ex) {
                throw new RepositoryException(repositoryName, "failed to get snapshot list in repository", ex);
            }
        }
    }

    @Override
    public String startVerification() {
        //TODO: #7831 Add check that URL exists and accessible
        return null;
    }

    @Override
    public void endVerification(String seed) {
        throw new UnsupportedOperationException("shouldn't be called");
    }

}
