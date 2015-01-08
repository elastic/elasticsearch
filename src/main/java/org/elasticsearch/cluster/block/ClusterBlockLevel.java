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

package org.elasticsearch.cluster.block;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.Version;

import java.util.EnumSet;

/**
 *
 */
public enum ClusterBlockLevel {
    READ(0),
    WRITE(1),

    /**
     * Since 1.6.0, METADATA has been split into two distincts cluster block levels
     * @deprecated Use METADATA_READ or METADATA_WRITE instead.
     */
    @Deprecated
    METADATA(2),

    METADATA_READ(3),
    METADATA_WRITE(4);

    public static final EnumSet<ClusterBlockLevel> ALL = EnumSet.of(READ, WRITE, METADATA_READ, METADATA_WRITE);
    public static final EnumSet<ClusterBlockLevel> READ_WRITE = EnumSet.of(READ, WRITE);

    private final int id;

    ClusterBlockLevel(int id) {
        this.id = id;
    }

    public int id() {
        return this.id;
    }

    /**
     * Returns the ClusterBlockLevel's id according to a given version, this to ensure backward compatibility.
     *
     * @param version the version
     * @return the ClusterBlockLevel's id
     */
    public int toId(Version version) {
        assert version != null : "Version shouldn't be null";
        // Since 1.6.0, METADATA has been split into two distincts cluster block levels
        if (version.before(Version.V_1_6_0)) {
            if (this == ClusterBlockLevel.METADATA_READ || this == ClusterBlockLevel.METADATA_WRITE) {
                return ClusterBlockLevel.METADATA.id();
            }
        }
        return id();
    }

    static EnumSet<ClusterBlockLevel> fromId(int id) {
        if (id == 0) {
            return EnumSet.of(READ);
        } else if (id == 1) {
            return EnumSet.of(WRITE);
        } else if (id == 2) {
            // Since 1.6.0, METADATA has been split into two distincts cluster block levels
            return EnumSet.of(METADATA_READ, METADATA_WRITE);
        } else if (id == 3) {
            return EnumSet.of(METADATA_READ);
        } else if (id == 4) {
            return EnumSet.of(METADATA_WRITE);
        }
        throw new ElasticsearchIllegalArgumentException("No cluster block level matching [" + id + "]");
    }
}
