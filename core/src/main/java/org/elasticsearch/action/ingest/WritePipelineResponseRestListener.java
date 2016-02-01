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

package org.elasticsearch.action.ingest;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

import java.io.IOException;

public class WritePipelineResponseRestListener extends AcknowledgedRestListener<WritePipelineResponse> {

    public WritePipelineResponseRestListener(RestChannel channel) {
        super(channel);
    }

    @Override
    protected void addCustomFields(XContentBuilder builder, WritePipelineResponse response) throws IOException {
        if (!response.isAcknowledged()) {
            response.getError().toXContent(builder, null);
        }
    }
}

