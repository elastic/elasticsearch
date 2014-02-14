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

package org.elasticsearch.river.routing;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.river.RiverName;

import java.io.IOException;
import java.util.Iterator;

/**
 *
 */
public class RiversRouting implements Iterable<RiverRouting> {

    public static final RiversRouting EMPTY = RiversRouting.builder().build();

    private final ImmutableMap<RiverName, RiverRouting> rivers;

    private RiversRouting(ImmutableMap<RiverName, RiverRouting> rivers) {
        this.rivers = rivers;
    }

    public boolean isEmpty() {
        return rivers.isEmpty();
    }

    public RiverRouting routing(RiverName riverName) {
        return rivers.get(riverName);
    }

    public boolean hasRiverByName(String name) {
        for (RiverName riverName : rivers.keySet()) {
            if (riverName.name().equals(name)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Iterator<RiverRouting> iterator() {
        return rivers.values().iterator();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private MapBuilder<RiverName, RiverRouting> rivers = MapBuilder.newMapBuilder();

        public Builder routing(RiversRouting routing) {
            rivers.putAll(routing.rivers);
            return this;
        }

        public Builder put(RiverRouting routing) {
            rivers.put(routing.riverName(), routing);
            return this;
        }

        public Builder remove(RiverRouting routing) {
            rivers.remove(routing.riverName());
            return this;
        }

        public Builder remove(RiverName riverName) {
            rivers.remove(riverName);
            return this;
        }

        public Builder remote(String riverName) {
            for (RiverName name : rivers.map().keySet()) {
                if (name.name().equals(riverName)) {
                    rivers.remove(name);
                }
            }
            return this;
        }

        public RiversRouting build() {
            return new RiversRouting(rivers.immutableMap());
        }

        public static RiversRouting readFrom(StreamInput in) throws IOException {
            Builder builder = new Builder();
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                builder.put(RiverRouting.readRiverRouting(in));
            }
            return builder.build();
        }

        public static void writeTo(RiversRouting routing, StreamOutput out) throws IOException {
            out.writeVInt(routing.rivers.size());
            for (RiverRouting riverRouting : routing) {
                riverRouting.writeTo(out);
            }
        }
    }
}
