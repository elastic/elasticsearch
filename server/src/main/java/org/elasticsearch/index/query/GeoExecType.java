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

package org.elasticsearch.index.query;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/** Specifies how a geo query should be run. */
public enum GeoExecType implements Writeable {
    
    MEMORY(0), INDEXED(1);

    private final int ordinal;

    GeoExecType(int ordinal) {
        this.ordinal = ordinal;
    }

    public static GeoExecType readFromStream(StreamInput in) throws IOException {
        int ord = in.readVInt();
        switch(ord) {
            case(0): return MEMORY;
            case(1): return INDEXED;
        }
        throw new ElasticsearchException("unknown serialized type [" + ord + "]");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(this.ordinal);
    }

    public static GeoExecType fromString(String typeName) {
        if (typeName == null) {
            throw new IllegalArgumentException("cannot parse type from null string");
        }

        for (GeoExecType type : GeoExecType.values()) {
            if (type.name().equalsIgnoreCase(typeName)) {
                return type;
            }
        }
        throw new IllegalArgumentException("no type can be parsed from ordinal " + typeName);
    }
}