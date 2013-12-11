/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.elasticsearch.rest;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

import static org.elasticsearch.rest.RestStatus.OK;

/**
 */
public class AcknowledgedRestResponseActionListener<T extends AcknowledgedResponse> extends AbstractRestResponseActionListener<T> {

    public AcknowledgedRestResponseActionListener(RestRequest request, RestChannel channel, ESLogger logger) {
        super(request, channel, logger);
    }

    @Override
    public void onResponse(T response) {
        try {
            XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
            builder.startObject()
                    .field(Fields.OK, true)
                    .field(Fields.ACKNOWLEDGED, response.isAcknowledged());
            addCustomFields(builder, response);
            builder.endObject();
            channel.sendResponse(new XContentRestResponse(request, OK, builder));
        } catch (IOException e) {
            onFailure(e);
        }
    }

    /**
     * Adds api specific fields to the rest response
     * Does nothing by default but can be overridden by subclasses
     */
    protected void addCustomFields(XContentBuilder builder, T response) throws IOException {

    }

    static final class Fields {
        static final XContentBuilderString OK = new XContentBuilderString("ok");
        static final XContentBuilderString ACKNOWLEDGED = new XContentBuilderString("acknowledged");
    }
}
