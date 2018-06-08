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

package org.elasticsearch.packaging.util;

import java.nio.file.Path;

/**
 * Represents an installation of Elasticsearch
 */
public class Installation {

    public final Path home;
    public final Path config;
    public final Path data;
    public final Path logs;
    public final Path plugins;
    public final Path modules;
    public final Path scripts;

    public Installation(Path home, Path config, Path data, Path logs, Path plugins, Path modules, Path scripts) {
        this.home = home;
        this.config = config;
        this.data = data;
        this.logs = logs;
        this.plugins = plugins;
        this.modules = modules;
        this.scripts = scripts;
    }

    public Installation(Path home) {
        this(
            home,
            home.resolve("config"),
            home.resolve("data"),
            home.resolve("logs"),
            home.resolve("plugins"),
            home.resolve("modules"),
            home.resolve("scripts")
        );
    }
}
