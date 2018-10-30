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

package org.elasticsearch.client.security.user;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

public final class ClusterPrivilege {

    private static final Pattern ALL_CLUSTER_PATTERN = Pattern.compile("^cluster:|indices:admin/template/");
    private static final Map<String, ClusterPrivilege> builtins = new HashMap<>();

    public static final ClusterPrivilege NONE = new ClusterPrivilege("none", true);
    public static final ClusterPrivilege ALL = new ClusterPrivilege("all", true);
    public static final ClusterPrivilege MONITOR = new ClusterPrivilege("monitor", true);
    public static final ClusterPrivilege MONITOR_ML = new ClusterPrivilege("monitor_ml", true);
    public static final ClusterPrivilege MONITOR_WATCHER = new ClusterPrivilege("monitor_watcher", true);
    public static final ClusterPrivilege MONITOR_ROLLUP = new ClusterPrivilege("monitor_rollup", true);
    public static final ClusterPrivilege MANAGE = new ClusterPrivilege("manage", true);
    public static final ClusterPrivilege MANAGE_ML = new ClusterPrivilege("manage_ml", true);
    public static final ClusterPrivilege MANAGE_WATCHER = new ClusterPrivilege("manage_watcher", true);
    public static final ClusterPrivilege MANAGE_ROLLUP = new ClusterPrivilege("manage_rollup", true);
    public static final ClusterPrivilege MANAGE_IDX_TEMPLATES = new ClusterPrivilege("manage_index_templates", true);
    public static final ClusterPrivilege MANAGE_INGEST_PIPELINES = new ClusterPrivilege("manage_ingest_pipelines", true);
    public static final ClusterPrivilege TRANSPORT_CLIENT = new ClusterPrivilege("transport_client", true);
    public static final ClusterPrivilege MANAGE_SECURITY = new ClusterPrivilege("manage_security", true);
    public static final ClusterPrivilege MANAGE_SAML = new ClusterPrivilege("manage_saml", true);
    public static final ClusterPrivilege MANAGE_PIPELINE = new ClusterPrivilege("manage_pipeline", true);
    public static final ClusterPrivilege MANAGE_CCR = new ClusterPrivilege("manage_ccr", true);
    public static final ClusterPrivilege READ_CCR = new ClusterPrivilege("read_ccr", true);

    private final String name;

    private ClusterPrivilege(String name, boolean builtin) {
        this.name = name;
        if (builtin) {
            builtins.put(name, this);
        }
    }

    public static ClusterPrivilege fromString(String name) {
        Objects.requireNonNull(name);
        final ClusterPrivilege builtin = builtins.get(name);
        if (builtin != null) {
            return builtin;
        }
        if (false == ALL_CLUSTER_PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException("[" + name + "] is not a cluster action privilege.");
        }
        return new ClusterPrivilege(name, false);
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || false == getClass().equals(o.getClass())) {
            return false;
        }
        return Objects.equals(name, ((ClusterPrivilege) o).name);
    }
}
