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
package org.elasticsearch.repositories.hdfs;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.hadoop.hdfs.blobstore.HdfsBlobStore;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryName;
import org.elasticsearch.repositories.RepositorySettings;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

public class HdfsRepository extends BlobStoreRepository implements Repository {

    public final static String TYPE = "hdfs";

    private final HdfsBlobStore blobStore;
    private final BlobPath basePath;
    private ByteSizeValue chunkSize;
    private boolean compress;
    private final ExecutorService concurrentStreamPool;
    private final FileSystem fs;

    @Inject
    public HdfsRepository(RepositoryName name, RepositorySettings repositorySettings, IndexShardRepository indexShardRepository) throws IOException {
        super(name.getName(), repositorySettings, indexShardRepository);

        String path = repositorySettings.settings().get("path", componentSettings.get("path"));
        if (path == null) {
            throw new ElasticsearchIllegalArgumentException("no 'path' defined for hdfs snapshot/restore");
        }

        // get configuration
        fs = initFileSystem(repositorySettings);
        Path hdfsPath = fs.makeQualified(new Path(path));
        this.basePath = BlobPath.cleanPath();

        int concurrentStreams = repositorySettings.settings().getAsInt("concurrent_streams", componentSettings.getAsInt("concurrent_streams", 5));
        concurrentStreamPool = EsExecutors.newScaling(1, concurrentStreams, 5, TimeUnit.SECONDS,
                EsExecutors.daemonThreadFactory(settings, "[hdfs_stream]"));

        logger.debug("Using file-system [{}] for URI [{}], path [{}], concurrent_streams [{}]", fs, fs.getUri(), hdfsPath, concurrentStreams);
        blobStore = new HdfsBlobStore(settings, fs, hdfsPath, concurrentStreamPool);
        this.chunkSize = repositorySettings.settings().getAsBytesSize("chunk_size", componentSettings.getAsBytesSize("chunk_size", null));
        this.compress = repositorySettings.settings().getAsBoolean("compress", componentSettings.getAsBoolean("compress", false));
    }

    private FileSystem initFileSystem(RepositorySettings repositorySettings) throws IOException {
        Configuration cfg = new Configuration(repositorySettings.settings().getAsBoolean("load_defaults", componentSettings.getAsBoolean("load_defaults", true)));

        String confLocation = repositorySettings.settings().get("conf_location", componentSettings.get("conf_location"));
        if (Strings.hasText(confLocation)) {
            for (String entry : Strings.commaDelimitedListToStringArray(confLocation)) {
                addConfigLocation(cfg, entry.trim());
            }
        }

        Map<String, String> map = componentSettings.getByPrefix("conf.").getAsMap();
        for (Entry<String, String> entry : map.entrySet()) {
            cfg.set(entry.getKey(), entry.getValue());
        }

        String uri = repositorySettings.settings().get("uri", componentSettings.get("uri"));
        URI actualUri = (uri != null ? URI.create(uri) : FileSystem.getDefaultUri(cfg));
        String user = repositorySettings.settings().get("user", componentSettings.get("user"));

        try {
            return (user != null ? FileSystem.get(actualUri, cfg, user) : FileSystem.get(actualUri, cfg));
        } catch (Exception ex) {
            throw new ElasticsearchGenerationException(String.format("Cannot create Hdfs file-system for uri [%s]", actualUri), ex);
        }
    }

    private void addConfigLocation(Configuration cfg, String confLocation) {
        URL cfgURL = null;
        // it's an URL
        if (!confLocation.contains(":")) {
            cfgURL = cfg.getClassLoader().getResource(confLocation);

            // fall back to file
            if (cfgURL == null) {
                File file = new File(confLocation);
                if (!file.canRead()) {
                    throw new ElasticsearchIllegalArgumentException(
                            String.format(
                                    "Cannot find classpath resource or file 'conf_location' [%s] defined for hdfs snapshot/restore",
                                    confLocation));
                }
                String fileLocation = file.toURI().toString();
                logger.debug("Adding path [{}] as file [{}]", confLocation, fileLocation);
                confLocation = fileLocation;
            }
            else {
                logger.debug("Resolving path [{}] to classpath [{}]", confLocation, cfgURL);
            }
        }
        else {
            logger.debug("Adding path [{}] as URL", confLocation);
        }

        if (cfgURL == null) {
            try {
                cfgURL = new URL(confLocation);
            } catch (MalformedURLException ex) {
                throw new ElasticsearchIllegalArgumentException(String.format(
                        "Invalid 'conf_location' URL [%s] defined for hdfs snapshot/restore", confLocation), ex);
            }
        }

        cfg.addResource(cfgURL);
    }

    @Override
    protected BlobStore blobStore() {
        return blobStore;
    }

    protected BlobPath basePath() {
        return basePath;
    }

    @Override
    protected boolean isCompress() {
        return compress;
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        super.doClose();

        IOUtils.closeStream(fs);
        concurrentStreamPool.shutdown();
    }
}