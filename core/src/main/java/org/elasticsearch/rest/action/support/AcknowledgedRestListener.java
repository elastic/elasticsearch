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
package org.elasticsearch.rest.action.support;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.support.RestActionListener;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import java.io.IOException;

import static org.elasticsearch.rest.RestStatus.OK;

/**
 */
public class AcknowledgedRestListener<T extends AcknowledgedResponse> extends RestBuilderListener<T> {

    public AcknowledgedRestListener(RestChannel channel) {
        super(channel);
    }

    @Override
    public RestResponse buildResponse(T response, XContentBuilder builder) throws Exception {
        builder.startObject()
                .field(Fields.ACKNOWLEDGED, response.isAcknowledged());
        addCustomFields(builder, response);
        builder.endObject();
        return new BytesRestResponse(OK, builder);
    }

    /**
     * Adds api specific fields to the rest response
     * Does nothing by default but can be overridden by subclasses
     */
    protected void addCustomFields(XContentBuilder builder, T response) throws IOException {

    }

    static final class Fields {
        static final XContentBuilderString ACKNOWLEDGED = new XContentBuilderString("acknowledged");
    }
}
