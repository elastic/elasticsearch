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

package org.elasticsearch.common.settings.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.loader.SettingsLoader;
import org.elasticsearch.common.settings.loader.SettingsLoaderFactory;
import org.elasticsearch.zookeeper.ZooKeeperEnvironment;
import org.elasticsearch.zookeeper.ZooKeeperFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author imotov
 */
public final class ZooKeeperSettingsLoader {

    private ZooKeeperSettingsLoader() {

    }

    public static Settings loadZooKeeperSettings(Settings settings) {
        ZooKeeperFactory zooKeeperFactory = new ZooKeeperFactory(settings);

        ClusterName clusterName = ClusterName.clusterNameFromSettings(settings);

        ZooKeeperEnvironment zooKeeperEnvironment = new ZooKeeperEnvironment(settings, clusterName);

        ZooKeeper zooKeeper = zooKeeperFactory.newZooKeeper();
        try {
            return ImmutableSettings.settingsBuilder()
                    .put(loadSettings(zooKeeper, zooKeeperEnvironment.globalSettingsNodePath()))
                    .put(loadSettings(zooKeeper, zooKeeperEnvironment.clusterSettingsNodePath()))
                    .build();
        } catch (InterruptedException e) {
            // Ignore
        } catch (KeeperException e) {
            throw new ElasticSearchException("Cannot load settings ", e);
        } finally {
            try {
                zooKeeper.close();
            } catch (InterruptedException ex) {
                // Ignore
            }
        }
        return ImmutableSettings.Builder.EMPTY_SETTINGS;
    }

    private static Map<String, String> loadSettings(ZooKeeper zooKeeper, String path) throws InterruptedException, KeeperException {
        try {
            byte[] settingsBytes = zooKeeper.getData(path, null, null);
            SettingsLoader loader = SettingsLoaderFactory.loaderFromSource(new String(settingsBytes));
            return loader.load(settingsBytes);
        } catch (KeeperException.NoNodeException e) {
            return Collections.emptyMap();
        } catch (IOException ex) {
            throw new ElasticSearchException("Cannot load settings ", ex);
        }
    }
}
