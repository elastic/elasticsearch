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

package org.elasticsearch.cluster.factory;

import org.elasticsearch.common.collect.ImmutableOpenMap;

public class ImmutableOpenMapClusterStatePartFactory<P> extends MultiClusterStatePartFactory<ImmutableOpenMap<String, P>, P> {

    private final ClusterStatePartFactory<P> factory;

    public ImmutableOpenMapClusterStatePartFactory(String type, ClusterStatePartFactory<P> factory) {
        super(type, factory.context(), factory.since());
        this.factory = factory;
    }


    @Override
    public ClusterStatePartFactory<P> lookupFactory(String type) {
        return factory;
    }

    @Override
    public ImmutableOpenMap<String, P> fromParts(ImmutableOpenMap<String, P> parts) {
        return parts;
    }

    @Override
    public ImmutableOpenMap<String, P> toParts(ImmutableOpenMap<String, P> parts) {
        return parts;
    }
}

