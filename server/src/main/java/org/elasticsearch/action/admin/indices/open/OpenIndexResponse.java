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

package org.elasticsearch.action.admin.indices.open;

import org.elasticsearch.action.support.master.ShardsAcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * A response for a open index action.
 */
public class OpenIndexResponse extends ShardsAcknowledgedResponse {

    private static final ConstructingObjectParser<OpenIndexResponse, Void> PARSER = new ConstructingObjectParser<>("open_index", true,
            args -> new OpenIndexResponse((boolean) args[0], (boolean) args[1]));

    static {
        declareAcknowledgedAndShardsAcknowledgedFields(PARSER);
    }

    public OpenIndexResponse(StreamInput in) throws IOException {
        super(in, true);
    }

    public OpenIndexResponse(boolean acknowledged, boolean shardsAcknowledged) {
        super(acknowledged, shardsAcknowledged);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeShardsAcknowledged(out);
    }

    public static OpenIndexResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
