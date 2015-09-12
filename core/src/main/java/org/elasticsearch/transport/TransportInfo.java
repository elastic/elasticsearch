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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class TransportInfo implements Streamable, ToXContent {

    private BoundTransportAddress address;
    private Map<String, BoundTransportAddress> profileAddresses;

    TransportInfo() {
    }

    public TransportInfo(BoundTransportAddress address, @Nullable Map<String, BoundTransportAddress> profileAddresses) {
        this.address = address;
        this.profileAddresses = profileAddresses;
    }

    static final class Fields {
        static final XContentBuilderString TRANSPORT = new XContentBuilderString("transport");
        static final XContentBuilderString BOUND_ADDRESS = new XContentBuilderString("bound_address");
        static final XContentBuilderString PUBLISH_ADDRESS = new XContentBuilderString("publish_address");
        static final XContentBuilderString PROFILES = new XContentBuilderString("profiles");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.TRANSPORT);
        builder.field(Fields.BOUND_ADDRESS, address.boundAddress().toString());
        builder.field(Fields.PUBLISH_ADDRESS, address.publishAddress().toString());
        builder.startObject(Fields.PROFILES);
        if (profileAddresses != null && profileAddresses.size() > 0) {
            for (Map.Entry<String, BoundTransportAddress> entry : profileAddresses.entrySet()) {
                builder.startObject(entry.getKey());
                builder.field(Fields.BOUND_ADDRESS, entry.getValue().boundAddress().toString());
                builder.field(Fields.PUBLISH_ADDRESS, entry.getValue().publishAddress().toString());
                builder.endObject();
            }
        }
        builder.endObject();
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
        int size = in.readVInt();
        if (size > 0) {
            profileAddresses = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                String key = in.readString();
                BoundTransportAddress value = BoundTransportAddress.readBoundTransportAddress(in);
                profileAddresses.put(key, value);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        address.writeTo(out);
        if (profileAddresses != null) {
            out.writeVInt(profileAddresses.size());
        } else {
            out.writeVInt(0);
        }
        if (profileAddresses != null && profileAddresses.size() > 0) {
            for (Map.Entry<String, BoundTransportAddress> entry : profileAddresses.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        }
    }

    public BoundTransportAddress address() {
        return address;
    }

    public BoundTransportAddress getAddress() {
        return address();
    }

    public Map<String, BoundTransportAddress> getProfileAddresses() {
        return profileAddresses();
    }

    public Map<String, BoundTransportAddress> profileAddresses() {
        return profileAddresses;
    }
}
