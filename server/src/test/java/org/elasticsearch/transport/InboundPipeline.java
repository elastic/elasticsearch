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

package org.elasticsearch.transport;

import org.elasticsearch.common.bytes.ReleasableBytesReference;

import java.io.IOException;
import java.util.ArrayList;

public class InboundPipeline {

    private final InboundDecoder2 decoder;
    private final InboundAggregator aggregator;

    public InboundPipeline(InboundDecoder2 decoder, InboundAggregator aggregator) {
        this.decoder = decoder;
        this.aggregator = aggregator;
    }

    public int handleBytes(ReleasableBytesReference reference) throws IOException {
        int bytesConsumed = 0;
        final ArrayList<Object> fragments = new ArrayList<>();
        while (true) {
            final int bytesDecoded = decoder.handle(reference, fragments::add);
            if (bytesDecoded == 0 || fragments.get(fragments.size() - 1) == InboundDecoder2.END_CONTENT) {
                break;
            }
        }

        return bytesConsumed;
    }
}
