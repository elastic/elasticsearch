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

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.io.Serializable;

/**
 *
 */
public class TransportInfo implements Streamable, Serializable, ToXContent {

    private BoundTransportAddress address;

    TransportInfo() {
    }

    public TransportInfo(BoundTransportAddress address) {
        this.address = address;
    }

    static final class Fields {
        static final XContentBuilderString TRANSPORT = new XContentBuilderString("transport");
        static final XContentBuilderString BOUND_ADDRESS = new XContentBuilderString("bound_address");
        static final XContentBuilderString PUBLISH_ADDRESS = new XContentBuilderString("publish_address");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.TRANSPORT);
        builder.field(Fields.BOUND_ADDRESS, address.boundAddress().toString());
        builder.field(Fields.PUBLISH_ADDRESS, address.publishAddress().toString());
        builder.endObject();
        return builder;
    }

    public static TransportInfo readTransportInfo(StreamInput in) throws IOException {
        TransportInfo info = new TransportInfo();
        info.readFrom(in);
        return info;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        address = BoundTransportAddress.readBoundTransportAddress(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        address.writeTo(out);
    }

    public BoundTransportAddress address() {
        return address;
    }

    public BoundTransportAddress getAddress() {
        return address();
    }
}
