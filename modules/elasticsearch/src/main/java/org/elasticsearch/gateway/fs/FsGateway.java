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

package org.elasticsearch.gateway.fs;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.gateway.GatewayException;
import org.elasticsearch.index.gateway.fs.FsIndexGatewayModule;
import org.elasticsearch.util.component.AbstractLifecycleComponent;
import org.elasticsearch.util.inject.Inject;
import org.elasticsearch.util.inject.Module;
import org.elasticsearch.util.io.FileSystemUtils;
import org.elasticsearch.util.settings.Settings;
import org.elasticsearch.util.xcontent.ToXContent;
import org.elasticsearch.util.xcontent.XContentFactory;
import org.elasticsearch.util.xcontent.XContentParser;
import org.elasticsearch.util.xcontent.XContentType;
import org.elasticsearch.util.xcontent.builder.BinaryXContentBuilder;

import java.io.*;

import static org.elasticsearch.util.io.FileSystemUtils.*;

/**
 * @author kimchy (shay.banon)
 */
public class FsGateway extends AbstractLifecycleComponent<Gateway> implements Gateway {

    private final Environment environment;

    private final ClusterName clusterName;

    private final String location;

    private final File gatewayHome;

    private volatile int currentIndex;

    @Inject public FsGateway(Settings settings, Environment environment, ClusterName clusterName) throws IOException {
        super(settings);
        this.clusterName = clusterName;
        this.environment = environment;

        this.location = componentSettings.get("location");

        this.gatewayHome = createGatewayHome(location, environment, clusterName);

        if (!gatewayHome.exists()) {
            throw new IOException("FsGateway location [" + gatewayHome + "] can't be created");
        }

        this.currentIndex = findLatestIndex(gatewayHome);
        logger.debug("Latest metadata found at index [" + currentIndex + "]");
    }

    @Override protected void doStart() throws ElasticSearchException {
    }

    @Override protected void doStop() throws ElasticSearchException {
    }

    @Override protected void doClose() throws ElasticSearchException {
    }

    public File gatewayHome() {
        return gatewayHome;
    }

    private static File createGatewayHome(String location, Environment environment, ClusterName clusterName) {
        File f;
        if (location != null) {
            // if its a custom location, append the cluster name to it just so we have unique
            // in case two clusters point to the same location
            f = new File(new File(location), clusterName.value());
        } else {
            // work already includes the cluster name
            f = new File(environment.workWithClusterFile(), "gateway");
        }
        if (f.exists() && f.isDirectory()) {
            return f;
        }
        boolean result;
        for (int i = 0; i < 5; i++) {
            result = f.mkdirs();
            if (result) {
                break;
            }
        }

        return f;
    }

    @Override public void write(MetaData metaData) throws GatewayException {
        try {
            final File file = new File(gatewayHome, "metadata-" + (currentIndex + 1));
            for (int i = 0; i < 5; i++) {
                if (file.createNewFile())
                    break;
            }
            if (!file.exists()) {
                throw new GatewayException("Failed to create new file [" + file + "]");
            }

            BinaryXContentBuilder builder = XContentFactory.contentBinaryBuilder(XContentType.JSON);
            builder.prettyPrint();
            builder.startObject();
            MetaData.Builder.toXContent(metaData, builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();

            FileOutputStream fileStream = new FileOutputStream(file);
            fileStream.write(builder.unsafeBytes(), 0, builder.unsafeBytesLength());
            fileStream.close();

            syncFile(file);

            currentIndex++;

            //delete old files.
            File[] oldFiles = gatewayHome.listFiles(new FilenameFilter() {
                @Override public boolean accept(File dir, String name) {
                    return name.startsWith("metadata-") && !name.equals(file.getName());
                }
            });

            for (File oldFile : oldFiles) {
                oldFile.delete();
            }

        } catch (IOException e) {
            throw new GatewayException("can't write new metadata file into the gateway", e);
        }
    }

    @Override public MetaData read() throws GatewayException {
        try {
            if (currentIndex == -1)
                return null;

            File file = new File(gatewayHome, "metadata-" + currentIndex);
            if (!file.exists()) {
                throw new GatewayException("can't find current metadata file");
            }
            return readMetaData(file);
        } catch (GatewayException e) {
            throw e;
        } catch (Exception e) {
            throw new GatewayException("can't read metadata file from the gateway", e);
        }
    }

    @Override public Class<? extends Module> suggestIndexGateway() {
        return FsIndexGatewayModule.class;
    }

    @Override public void reset() {
        FileSystemUtils.deleteRecursively(gatewayHome, false);
        currentIndex = -1;
    }

    private int findLatestIndex(File gatewayHome) {
        File[] files = gatewayHome.listFiles(new FilenameFilter() {
            @Override public boolean accept(File dir, String name) {
                return name.startsWith("metadata-");
            }
        });

        int index = -1;
        for (File file : files) {
            if (logger.isTraceEnabled()) {
                logger.trace("[findLatestMetadata]: Processing file [" + file + "]");
            }
            String name = file.getName();
            int fileIndex = Integer.parseInt(name.substring(name.indexOf('-') + 1));
            if (fileIndex >= index) {
                // try and read the meta data
                try {
                    readMetaData(file);
                    index = fileIndex;
                } catch (IOException e) {
                    logger.warn("[findLatestMetadata]: Failed to read metadata from [" + file + "], ignoring...", e);
                }
            }
        }

        return index;
    }

    private MetaData readMetaData(File file) throws IOException {
        FileInputStream fileStream = new FileInputStream(file);
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
