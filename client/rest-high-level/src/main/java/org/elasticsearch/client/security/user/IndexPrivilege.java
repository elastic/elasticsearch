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

import java.util.Objects;
import java.util.regex.Pattern;

public class IndexPrivilege {

    private static final Pattern ALL_INDEX_PATTERN = Pattern.compile("^indices:|internal:transport/proxy/indices:");

    public static final IndexPrivilege NONE = new IndexPrivilege("none");
    public static final IndexPrivilege ALL = new IndexPrivilege("all");
    public static final IndexPrivilege READ = new IndexPrivilege("read");
    public static final IndexPrivilege READ_CROSS_CLUSTER = new IndexPrivilege("read_cross_cluster");
    public static final IndexPrivilege CREATE = new IndexPrivilege("create");
    public static final IndexPrivilege INDEX = new IndexPrivilege("index");
    public static final IndexPrivilege DELETE = new IndexPrivilege("delete");
    public static final IndexPrivilege WRITE = new IndexPrivilege("write");
    public static final IndexPrivilege MONITOR = new IndexPrivilege("monitor");
    public static final IndexPrivilege MANAGE = new IndexPrivilege("manage");
    public static final IndexPrivilege DELETE_INDEX = new IndexPrivilege("delete_index");
    public static final IndexPrivilege CREATE_INDEX = new IndexPrivilege("create_index");
    public static final IndexPrivilege VIEW_METADATA = new IndexPrivilege("view_index_metadata");
    public static final IndexPrivilege MANAGE_FOLLOW_INDEX = new IndexPrivilege("manage_follow_index");

    private final String name;

    private IndexPrivilege(String name) {
        this.name = name;
    }

    public static IndexPrivilege custom(String name) {
        Objects.requireNonNull(name);
        if (false == ALL_INDEX_PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException("[" + name + "] is not an index action privilege.");
        }
        return new IndexPrivilege(name);
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
        return Objects.equals(name, ((IndexPrivilege) o).name);
    }
}
