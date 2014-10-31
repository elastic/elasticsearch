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

package org.elasticsearch.search.reducers;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class ReducerFactories implements Streamable {

    public static final ReducerFactories EMPTY = new Empty();

    private ReducerFactory[] factories;

    public ReducerFactories() {
        // For Serialization
    }

    private ReducerFactories(ReducerFactory[] factories) {
        this.factories = factories;
    }

    public Reducer[] createSubReducers(Reducer parent) {
        Reducer[] reducers = new Reducer[factories.length];
        for (int i = 0; i < factories.length; i++) {
            reducers[i] = factories[i].create(parent.context(), parent);
        }
        return reducers;
    }

    public Reducer[] createTopLevelReducers(ReducerContext context) {
        Reducer[] reducers = new Reducer[factories.length];
        for (int i = 0; i < factories.length; i++) {
            reducers[i] = factories[i].create(context, null);
        }
        return reducers;
    }

    public void validate() {
        for (ReducerFactory factory : factories) {
            factory.validate();
        }
    }

    private final static class Empty extends ReducerFactories {

        private static final ReducerFactory[] EMPTY_FACTORIES = new ReducerFactory[0];
        private static final Reducer[] EMPTY_AGGREGATORS = new Reducer[0];

        private Empty() {
            super(EMPTY_FACTORIES);
        }

        @Override
        public Reducer[] createSubReducers(Reducer parent) {
            return EMPTY_AGGREGATORS;
        }

        @Override
        public Reducer[] createTopLevelReducers(ReducerContext context) {
            return EMPTY_AGGREGATORS;
        }

    }

    public static class Builder {

        private final Set<String> names = new HashSet<>();
        private final List<ReducerFactory> factories = new ArrayList<>();

        public Builder add(ReducerFactory factory) {
            if (!names.add(factory.name)) {
                throw new ElasticsearchIllegalArgumentException("Two sibling aggregations cannot have the same name: [" + factory.name + "]");
            }
            factories.add(factory);
            return this;
        }

        public ReducerFactories build() {
            if (factories.isEmpty()) {
                return EMPTY;
            }
            return new ReducerFactories(factories.toArray(new ReducerFactory[factories.size()]));
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readInt();
        ReducerFactory[] factories = new ReducerFactory[size];
        for (int i = 0; i < factories.length; i++) {
            BytesReference type = in.readBytesReference();
            factories[i] = ReducerFactoryStreams.stream(type).readResult(in);
        }
        this.factories = factories;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(factories.length);
        for (ReducerFactory factory : factories) {
            out.writeBytesReference(factory.type().stream());
            factory.writeTo(out);
        }
    }

    public static ReducerFactories readReducerFactories(StreamInput in) throws IOException {
        ReducerFactories factories = new ReducerFactories();
        factories.readFrom(in);
        return factories;
    }
}
