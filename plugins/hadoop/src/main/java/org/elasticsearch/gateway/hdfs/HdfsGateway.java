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

package org.elasticsearch.gateway.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.gateway.GatewayException;
import org.elasticsearch.util.component.AbstractLifecycleComponent;
import org.elasticsearch.util.inject.Inject;
import org.elasticsearch.util.inject.Module;
import org.elasticsearch.util.settings.Settings;
import org.elasticsearch.util.xcontent.ToXContent;
import org.elasticsearch.util.xcontent.XContentFactory;
import org.elasticsearch.util.xcontent.XContentParser;
import org.elasticsearch.util.xcontent.XContentType;
import org.elasticsearch.util.xcontent.builder.BinaryXContentBuilder;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class HdfsGateway extends AbstractLifecycleComponent<Gateway> implements Gateway {

    private final boolean closeFileSystem;

    private final FileSystem fileSystem;

    private final String uri;

    private final Path path;

    private final Path metaDataPath;

    private volatile int currentIndex;

    @Inject public HdfsGateway(Settings settings, ClusterName clusterName) throws IOException {
        super(settings);

        this.closeFileSystem = componentSettings.getAsBoolean("close_fs", true);
        this.uri = componentSettings.get("uri");
        if (uri == null) {
            throw new ElasticSearchIllegalArgumentException("hdfs gateway requires the 'uri' setting to be set");
        }
        String path = componentSettings.get("path");
        if (path == null) {
            throw new ElasticSearchIllegalArgumentException("hdfs gateway requires the 'path' path setting to be set");
        }
        this.path = new Path(new Path(path), clusterName.value());

        logger.debug("Using uri [{}], path [{}]", this.uri, this.path);

        this.metaDataPath = new Path(this.path, "metadata");

        Configuration conf = new Configuration();
        Settings hdfsSettings = settings.getByPrefix("hdfs.conf.");
        for (Map.Entry<String, String> entry : hdfsSettings.getAsMap().entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }

        fileSystem = FileSystem.get(URI.create(uri), conf);

        fileSystem.mkdirs(metaDataPath);

        this.currentIndex = findLatestIndex();
        logger.debug("Latest metadata found at index [" + currentIndex + "]");
    }

    public FileSystem fileSystem() {
        return this.fileSystem;
    }

    public Path path() {
        return this.path;
    }

    @Override protected void doStart() throws ElasticSearchException {
    }

    @Override protected void doStop() throws ElasticSearchException {
    }

    @Override protected void doClose() throws ElasticSearchException {
        if (closeFileSystem) {
            try {
                fileSystem.close();
            } catch (IOException e) {
                logger.warn("Failed to close file system {}", fileSystem);
            }
        }
    }

    @Override public void write(MetaData metaData) throws GatewayException {
        try {
            final Path file = new Path(metaDataPath, "metadata-" + (currentIndex + 1));

            BinaryXContentBuilder builder = XContentFactory.contentBinaryBuilder(XContentType.JSON);
            builder.prettyPrint();
            builder.startObject();
            MetaData.Builder.toXContent(metaData, builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();

            FSDataOutputStream fileStream = fileSystem.create(file, true);
            fileStream.write(builder.unsafeBytes(), 0, builder.unsafeBytesLength());
            fileStream.flush();
            fileStream.sync();
            fileStream.close();

            currentIndex++;

            FileStatus[] oldFiles = fileSystem.listStatus(metaDataPath, new PathFilter() {
                @Override public boolean accept(Path path) {
                    return path.getName().startsWith("metadata-") && !path.getName().equals(file.getName());
                }
            });

            if (oldFiles != null) {
                for (FileStatus oldFile : oldFiles) {
                    fileSystem.delete(oldFile.getPath(), false);
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

            Path file = new Path(metaDataPath, "metadata-" + currentIndex);
            return readMetaData(file);
        } catch (GatewayException e) {
            throw e;
        } catch (Exception e) {
            throw new GatewayException("can't read metadata file from the gateway", e);
        }
    }

    @Override public Class<? extends Module> suggestIndexGateway() {
        return HdfsIndexGatewayModule.class;
    }

    @Override public void reset() throws IOException {
        fileSystem.delete(path, true);
    }

    private int findLatestIndex() throws IOException {
        FileStatus[] files = fileSystem.listStatus(metaDataPath, new PathFilter() {
            @Override public boolean accept(Path path) {
                return path.getName().startsWith("metadata-");
            }
        });
        if (files == null || files.length == 0) {
            return -1;
        }

        int index = -1;
        for (FileStatus file : files) {
            if (logger.isTraceEnabled()) {
                logger.trace("[findLatestMetadata]: Processing file [" + file + "]");
            }
            String name = file.getPath().getName();
            int fileIndex = Integer.parseInt(name.substring(name.indexOf('-') + 1));
            if (fileIndex >= index) {
                // try and read the meta data
                try {
                    readMetaData(file.getPath());
                    index = fileIndex;
                } catch (Exception e) {
                    logger.warn("[findLatestMetadata]: Failed to read metadata from [" + file + "], ignoring...", e);
                }
            }
        }

        return index;
    }

    private MetaData readMetaData(Path file) throws IOException {
        FSDataInputStream fileStream = fileSystem.open(file);
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(XContentType.JSON).createParser(fileStream);
            return MetaData.Builder.fromXContent(parser, settings);
        } finally {
            if (parser != null) {
                parser.close();
            }
            try {
                fileStream.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }
}
