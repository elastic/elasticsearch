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

package org.elasticsearch.discovery.zookeeper;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zookeeper.embedded.EmbeddedZooKeeperService;
import org.elasticsearch.env.Environment;
import org.elasticsearch.zookeeper.ZooKeeperClient;
import org.elasticsearch.zookeeper.ZooKeeperClientService;
import org.elasticsearch.zookeeper.ZooKeeperEnvironment;
import org.elasticsearch.zookeeper.ZooKeeperFactory;
import org.testng.annotations.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 * @author imotov
 */
public abstract class AbstractZooKeeperTests {

    protected final ESLogger logger = Loggers.getLogger(getClass());

    protected EmbeddedZooKeeperService embeddedZooKeeperService;

    protected final List<ZooKeeperClient> zooKeeperClients = new ArrayList<ZooKeeperClient>();

    private Settings defaultSettings = ImmutableSettings.Builder.EMPTY_SETTINGS;

    private ZooKeeperEnvironment environment;

    private ZooKeeperFactory zooKeeperFactory;

    public void putDefaultSettings(Settings.Builder settings) {
        putDefaultSettings(settings.build());
    }

    public void putDefaultSettings(Settings settings) {
        defaultSettings = ImmutableSettings.settingsBuilder().put(defaultSettings).put(settings).build();
    }


    @BeforeClass public void startZooKeeper() throws IOException, InterruptedException {
        Environment tempEnvironment = new Environment(defaultSettings);
        File zooKeeperDataDirectory = new File(tempEnvironment.dataFile(), "zookeeper");
        logger.info("Deleting zookeeper directory {}", zooKeeperDataDirectory);
        deleteDirectory(zooKeeperDataDirectory);
        embeddedZooKeeperService = new EmbeddedZooKeeperService(defaultSettings, tempEnvironment);
        embeddedZooKeeperService.start();
        putDefaultSettings(ImmutableSettings.settingsBuilder()
                .put(defaultSettings)
                .put("zookeeper.host", "localhost:" + embeddedZooKeeperService.port()));

        zooKeeperFactory = new ZooKeeperFactory(defaultSettings);
        environment = new ZooKeeperEnvironment(defaultSettings, ClusterName.clusterNameFromSettings(defaultSettings));
    }

    @AfterClass public void stopZooKeeper() {
        if (embeddedZooKeeperService != null) {
            embeddedZooKeeperService.stop();
            embeddedZooKeeperService.close();
            embeddedZooKeeperService = null;
        }
    }

    @AfterMethod public void stopZooKeeperClients() {
        for (ZooKeeperClient zooKeeperClient : zooKeeperClients) {
            logger.info("Closing {}" + zooKeeperClient);
            zooKeeperClient.stop();
            zooKeeperClient.close();
        }
        zooKeeperClients.clear();
    }

    public ZooKeeperClient buildZooKeeper() {
        return buildZooKeeper(ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    public ZooKeeperClient buildZooKeeper(Settings settings) {
        String settingsSource = getClass().getName().replace('.', '/') + ".yml";
        Settings finalSettings = settingsBuilder()
                .loadFromClasspath(settingsSource)
                .put(defaultSettings)
                .put(settings)
                .build();
        ZooKeeperEnvironment environment = new ZooKeeperEnvironment(finalSettings, ClusterName.clusterNameFromSettings(defaultSettings));
        ZooKeeperClient zooKeeperClient = new ZooKeeperClientService(finalSettings, environment, zooKeeperFactory);
        zooKeeperClient.start();
        zooKeeperClients.add(zooKeeperClient);
        return zooKeeperClient;
    }

    public ZooKeeperFactory zooKeeperFactory() {
        return zooKeeperFactory;
    }

    public ZooKeeperEnvironment zooKeeperEnvironment() {
        return environment;
    }

    public Settings defaultSettings() {
        return defaultSettings;
    }


    private boolean deleteDirectory(File path) {
        if (path.exists()) {
            File[] files = path.listFiles();
            for (File file : files) {
                if (file.isDirectory()) {
                    if (!deleteDirectory(file)) {
                        return false;
                    }
                } else {
                    if (!file.delete()) {
                        return false;
                    }
                }
            }
            return path.delete();
        }
        return false;
    }
}
