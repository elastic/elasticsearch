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

package org.elasticsearch.river.fs;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.action.bulk.BulkRequestBuilder;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

import static java.util.Arrays.asList;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.river.fs.CollectionsCreator.*;
import static org.elasticsearch.river.fs.RiverSettingsExtractor.getSettingsGroup;

/**
 * @author lucastorri
 */
public class FSRiver extends AbstractRiverComponent implements River {

    private final ThreadPool threadPool;

    private final Client client;

    private final long checkInterval;

    private final String indexName;

    private final String typeName;

    private final File dir;

    private ScheduledFuture<?> scheduledCheck;

    private volatile boolean running;

    @Inject public FSRiver(RiverName riverName, RiverSettings settings, Client client, ThreadPool threadPool) {
        super(riverName, settings);
        this.client = client;
        this.threadPool = threadPool;

        Map<String, Object> fsSettings = getSettingsGroup(settings, "fs");
        String path = XContentMapValues.nodeStringValue(fsSettings.get("path"), "");
        dir = new File(path);
        checkInterval = XContentMapValues.nodeLongValue(fsSettings.get("check_interval"), 300000);

        Map<String, Object> indexSettings = getSettingsGroup(settings, "index");
        indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), riverName.name());
        typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), "status");

        running = false;
    }

    @Override public void start() {
        if (!dir.isDirectory()) {
            logger.warn("disabling fs river, path is not valid...");
            return;
        }
        logger.info("starting fs river");

        try {
            running = true;
            createIndex();
            monitorFiles();
        } catch (Exception e) {
            running = false;
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                // that's fine
            } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
                // ok, not recovered yet..., lets start indexing and hope we recover by the first bulk
                // TODO: a smarter logic can be to register for cluster event listener here, and only start sampling when the block is removed...
            } else {
                logger.warn("failed to create index [{}], disabling river...", e, indexName);
            }
        }
    }

    private void updateIndexes(Set<File> toIndex, Set<String> toRemove) {
        if (toIndex.isEmpty() && toRemove.isEmpty()) {
            logger.debug("no files to be updated");
            return;
        }

        BulkRequestBuilder bulk = client.prepareBulk();

        insertIndexes(toIndex, bulk);
        removeIndexes(toRemove, bulk);

        bulk.execute(new ActionListener<BulkResponse>() {
            @Override public void onResponse(BulkResponse bulkResponse) {
                logger.debug("files successfully updated");
            }

            @Override public void onFailure(Throwable e) {
                logger.warn("failed to execute bulk", e);
            }
        });
    }

    private void removeIndexes(Set<String> toRemove, BulkRequestBuilder bulk) {
        for (String f : toRemove) {
            bulk.add(Requests.deleteRequest(indexName).type(typeName).id(fileId(f)));
        }
    }

    private void insertIndexes(Set<File> toIndex, BulkRequestBuilder bulk) {
        for (File f : toIndex) {
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder().
                    startObject().
                        field("path", f.getPath()).
                        field("contents", fileContents(f)).
                    endObject();

                IndexRequest req = Requests.indexRequest(indexName).type(typeName).id(fileId(f));
                bulk.add(req.create(false).source(builder));
            } catch (IOException e) {
                logger.warn("could not index file {}", e, f.getPath());
            }
        }
    }

    private String fileId(String filePath) {
        String id = filePath.replaceAll(dir.getPath(), "");
        if (id.startsWith("/")) {
            id = id.substring(1);
        }
        return id;
    }

    private String fileId(File f) {
        return fileId(f.getPath());
    }

    private String fileContents(File f) throws IOException {
        FileInputStream fis = new FileInputStream(f);
        try {
            FileChannel fc = fis.getChannel();
            MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
            return Charset.defaultCharset().decode(bb).toString();
        } finally {
            fis.close();
        }
    }

    private void monitorFiles() {
        scheduledCheck = threadPool.scheduleWithFixedDelay(new Runnable() {

            private Map<String, Long> filesTimestamp = newMap();

            @Override public void run() {
                if (!running) {
                    return;
                }

                logger.debug("checking files");
                List<File> files = asList(dir.listFiles());
                Map<String, Long> newFilesTimestamp = newMap();
                Set<File> toIndex = new HashSet<File>();
                for (File f : files) {
                    final String path = f.getPath();
                    final Long lastModified = f.lastModified();
                    if (!lastModified.equals(filesTimestamp.get(path))) {
                        toIndex.add(f);
                    }
                    newFilesTimestamp.put(path, lastModified);
                }
                Set<String> toRemove = newSet(filesTimestamp.keySet());
                toRemove.removeAll(newFilesTimestamp.keySet());
                updateIndexes(toIndex, toRemove);
                filesTimestamp = newFilesTimestamp;
            }

        }, timeValueMillis(checkInterval));
    }

    private void createIndex() throws IOException {
        client.admin().indices().prepareCreate(indexName).execute().actionGet();//.addMapping(typeName, mapping);
    }

    @Override public void close() {
        running = false;
        logger.info("closing fs river");
        if (scheduledCheck != null) {
            scheduledCheck.cancel(true);
        }
    }

}
