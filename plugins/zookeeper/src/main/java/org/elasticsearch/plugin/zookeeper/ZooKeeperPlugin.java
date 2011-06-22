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

package org.elasticsearch.plugin.zookeeper;

import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.zookeeper.ZooKeeperSettingsLoader;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.zookeeper.ZooKeeperModule;

import java.util.Collection;
import java.util.Map;

/**
 * @author imotov
 */
public class ZooKeeperPlugin extends AbstractPlugin {

    private final Settings settings;

    public ZooKeeperPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override public String name() {
        return "zookeeper";
    }

    @Override public String description() {
        return "ZooKeeper Plugin";
    }

    @Override public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = Lists.newArrayList();
        if (settings.getAsBoolean("zookeeper.enabled", false)) {
            modules.add(ZooKeeperModule.class);
        }
        return modules;
    }

    @Override public Settings additionalSettings() {
        if (settings.getAsBoolean("zookeeper.settings.enabled", false)) {
            return ZooKeeperSettingsLoader.loadZooKeeperSettings(settings);
        } else {
            return super.additionalSettings();
        }
    }
}
