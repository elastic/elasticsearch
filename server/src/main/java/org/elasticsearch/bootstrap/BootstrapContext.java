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
package org.elasticsearch.bootstrap;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

/**
 * Context that is passed to every bootstrap check to make decisions on.
 */
public class BootstrapContext {
    /**
     * The node's environment
     */
    private final Environment environment;

    /**
     * The node's local state metadata loaded on startup
     */
    private final Metadata metadata;

    public BootstrapContext(Environment environment, Metadata metadata) {
        this.environment = environment;
        this.metadata = metadata;
    }

    public Environment environment() {
        return environment;
    }

    public Settings settings() {
        return environment.settings();
    }

    public Metadata metadata() {
        return metadata;
    }
}
