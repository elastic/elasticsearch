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
package org.elasticsearch.client.benchmark.ops.bulk;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.SuppressForbidden;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class LoadGenerator {
    private final Path bulkDataFile;
    private final BlockingQueue<List<String>> bulkQueue;
    private final int bulkSize;

    public LoadGenerator(Path bulkDataFile, BlockingQueue<List<String>> bulkQueue, int bulkSize) {
        this.bulkDataFile = bulkDataFile;
        this.bulkQueue = bulkQueue;
        this.bulkSize = bulkSize;
    }

    @SuppressForbidden(reason = "Classic I/O is fine in non-production code (and reading line by line is more ")
    public void execute() {
        try (BufferedReader reader = Files.newBufferedReader(bulkDataFile, StandardCharsets.UTF_8)) {
            String line;
            int bulkIndex = 0;
            List<String> bulkData = new ArrayList<>(bulkSize);
            while ((line = reader.readLine()) != null) {
                if (bulkIndex == bulkSize) {
                    // send bulk
                    sendBulk(bulkData);
                    // reset data structures
                    bulkData = new ArrayList<>(bulkSize);
                    bulkIndex = 0;
                }
                bulkData.add(line);
                bulkIndex++;
            }
            // also send the last bulk:
            if (bulkIndex > 0) {
                sendBulk(bulkData);
            }
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void sendBulk(List<String> bulkData) throws InterruptedException {
        bulkQueue.put(bulkData);
    }
}
