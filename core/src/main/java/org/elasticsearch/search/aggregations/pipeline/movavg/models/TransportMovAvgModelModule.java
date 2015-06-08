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

package org.elasticsearch.search.aggregations.pipeline.movavg.models;

import com.google.common.collect.Lists;
import org.elasticsearch.common.inject.AbstractModule;

import java.util.List;

/**
 * Register the transport streams so that models can be serialized/deserialized from the stream
 */
public class TransportMovAvgModelModule extends AbstractModule {

    private List<MovAvgModelStreams.Stream> streams = Lists.newArrayList();

    public TransportMovAvgModelModule() {
        registerStream(SimpleModel.STREAM);
        registerStream(LinearModel.STREAM);
        registerStream(EwmaModel.STREAM);
        registerStream(HoltLinearModel.STREAM);
        registerStream(HoltWintersModel.STREAM);
    }

    public void registerStream(MovAvgModelStreams.Stream stream) {
        streams.add(stream);
    }

    @Override
    protected void configure() {
        for (MovAvgModelStreams.Stream stream : streams) {
            MovAvgModelStreams.registerStream(stream, stream.getName());
        }
    }
}
