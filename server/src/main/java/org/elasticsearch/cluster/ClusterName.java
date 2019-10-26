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

package org.elasticsearch.cluster;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Predicate;

public class ClusterName implements Writeable {

    public static final Setting<ClusterName> CLUSTER_NAME_SETTING = new Setting<>("cluster.name", "elasticsearch", (s) -> {
        if (s.isEmpty()) {
            throw new IllegalArgumentException("[cluster.name] must not be empty");
        }
        if (s.contains(":")) {
            throw new IllegalArgumentException("[cluster.name] must not contain ':'");
        }
        return new ClusterName(s);
    }, Setting.Property.NodeScope);

    public static final ClusterName DEFAULT = CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY);

    private final String value;

    public ClusterName(StreamInput input) throws IOException {
        this(input.readString());
    }
    public ClusterName(String value) {
        this.value = value.intern();
    }

    public String value() {
        return this.value;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClusterName that = (ClusterName) o;

        if (value != null ? !value.equals(that.value) : that.value != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return "Cluster [" + value + "]";
    }

    public Predicate<ClusterName> getEqualityPredicate() {
        return new Predicate<ClusterName>() {
            @Override
            public boolean test(ClusterName o) {
                return ClusterName.this.equals(o);
            }

            @Override
            public String toString() {
                return "local cluster name [" + ClusterName.this.value() + "]";
            }
        };
    }
}
