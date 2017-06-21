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

package org.elasticsearch.repositories.s3;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.repositories.ESBlobStoreContainerTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Locale;

public class S3BlobStoreContainerTests extends ESBlobStoreContainerTestCase {

    private static final Logger logger = Loggers.getLogger(S3BlobStoreContainerTests.class);

    private static ServerSocket socket;

    @BeforeClass
    public static void openPort() throws IOException {
        socket = new ServerSocket(9200, 50, InetAddress.getByName("127.0.0.1"));
        new Thread(() -> {
            while (!socket.isClosed()) {
                try {
                    socket.accept();
                } catch (IOException e) {
                    logger.log(Level.ERROR, e);
                }
            }
        }).start();
    }

    protected BlobStore newBlobStore() throws IOException {
        MockAmazonS3 client = new MockAmazonS3();
        String bucket = randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);

        return new S3BlobStore(Settings.EMPTY, client, bucket, false,
            new ByteSizeValue(10, ByteSizeUnit.MB), "public-read-write", "standard");
    }

    @AfterClass
    public static void closePort() throws IOException {
        socket.close();
    }
}
