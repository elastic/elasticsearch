/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugins;

import java.util.Locale;

/**
 * Indicates the type of an Elasticsearch plugin.
 * <p>
 * Elasticsearch plugins come in two flavours: "isolated", which are kept
 * separate from the rest of the Elasticsearch code; and "bootstrap", which
 * take effect when Elasticsearch executes and can modify e.g. JVM
 * behaviour, but do not otherwise hook into the Elasticsearch lifecycle.
 */
public enum PluginType {
    ISOLATED,
    BOOTSTRAP;

    @Override
    public String toString() {
        return this.name().toLowerCase(Locale.ROOT);
    }
}
