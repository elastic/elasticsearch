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

package org.elasticsearch.groovy.node

import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.settings.loader.JsonSettingsLoader
import org.elasticsearch.groovy.common.xcontent.GXContentBuilder
import org.elasticsearch.node.Node
import org.elasticsearch.node.internal.InternalNode

/**
 * The node builder allow to build a {@link GNode} instance.
 *
 * @author kimchy (shay.banon)
 */
class GNodeBuilder {

    private final ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder()

    private boolean loadConfigSettings = true

    static GNodeBuilder nodeBuilder() {
        new GNodeBuilder()
    }

    ImmutableSettings.Builder getSettings() {
        settingsBuilder
    }

    ImmutableSettings.Builder settings(Settings.Builder settings) {
        settingsBuilder.put(settings.build())
    }

    ImmutableSettings.Builder settings(Settings settings) {
        settingsBuilder.put(settings)
    }

    ImmutableSettings.Builder settings(Closure settings) {
        byte[] settingsBytes = new GXContentBuilder().buildAsBytes(settings)
        settingsBuilder.put(new JsonSettingsLoader().load(settingsBytes))
    }

    GNode build() {
        Node node = new InternalNode(settingsBuilder.build(), loadConfigSettings)
        new GNode(node)
    }

    GNode node() {
        build().start()
    }
}
