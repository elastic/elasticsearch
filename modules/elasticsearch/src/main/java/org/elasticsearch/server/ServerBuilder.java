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

package org.elasticsearch.server;

import org.elasticsearch.server.internal.InternalServer;
import org.elasticsearch.util.settings.ImmutableSettings;
import org.elasticsearch.util.settings.Settings;

/**
 * @author kimchy (Shay Banon)
 */
public class ServerBuilder {

    private Settings settings = ImmutableSettings.Builder.EMPTY_SETTINGS;

    private boolean loadConfigSettings = true;

    public static ServerBuilder serverBuilder() {
        return new ServerBuilder();
    }

    public ServerBuilder settings(Settings.Builder settings) {
        return settings(settings.build());
    }

    public ServerBuilder settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    public ServerBuilder loadConfigSettings(boolean loadConfigSettings) {
        this.loadConfigSettings = loadConfigSettings;
        return this;
    }

    /**
     * Builds the server without starting it.
     */
    public Server build() {
        return new InternalServer(settings, loadConfigSettings);
    }

    /**
     * {@link #build()}s and starts the server.
     */
    public Server server() {
        return build().start();
    }
}
