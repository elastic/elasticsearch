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

package org.elasticsearch.search.rescore;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

public enum QueryRescoreMode implements Writeable {
    Avg {
        @Override
        public float combine(float primary, float secondary) {
            return (primary + secondary) / 2;
        }

        @Override
        public String toString() {
            return "avg";
        }
    },
    Max {
        @Override
        public float combine(float primary, float secondary) {
            return Math.max(primary, secondary);
        }

        @Override
        public String toString() {
            return "max";
        }
    },
    Min {
        @Override
        public float combine(float primary, float secondary) {
            return Math.min(primary, secondary);
        }

        @Override
        public String toString() {
            return "min";
        }
    },
    Total {
        @Override
        public float combine(float primary, float secondary) {
            return primary + secondary;
        }

        @Override
        public String toString() {
            return "sum";
        }
    },
    Multiply {
        @Override
        public float combine(float primary, float secondary) {
            return primary * secondary;
        }

        @Override
        public String toString() {
            return "product";
        }
    };

    public abstract float combine(float primary, float secondary);

    public static QueryRescoreMode readFromStream(StreamInput in) throws IOException {
        return in.readEnum(QueryRescoreMode.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    public static QueryRescoreMode fromString(String scoreMode) {
        for (QueryRescoreMode mode : values()) {
            if (scoreMode.toLowerCase(Locale.ROOT).equals(mode.name().toLowerCase(Locale.ROOT))) {
                return mode;
            }
        }
        throw new IllegalArgumentException("illegal score_mode [" + scoreMode + "]");
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
