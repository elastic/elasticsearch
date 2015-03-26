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

package org.elasticsearch.search.aggregations.reducers.smooth.models;

import com.google.common.collect.Lists;
import org.elasticsearch.common.inject.AbstractModule;

import java.util.List;

/**
 * Register the transport streams so that models can be serialized/deserialized from the stream
 */
public class TransportSmoothingModelModule extends AbstractModule {

    private List<SmoothingModelStreams.Stream> streams = Lists.newArrayList();

    public TransportSmoothingModelModule() {
        registerStream(SimpleModel.STREAM);
        registerStream(LinearModel.STREAM);
        registerStream(SingleExpModel.STREAM);
        registerStream(DoubleExpModel.STREAM);
    }

    public void registerStream(SmoothingModelStreams.Stream stream) {
        streams.add(stream);
    }

    @Override
    protected void configure() {
        for (SmoothingModelStreams.Stream stream : streams) {
            SmoothingModelStreams.registerStream(stream, stream.getName());
        }
    }
}
