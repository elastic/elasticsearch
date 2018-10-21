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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TransportInfo implements Writeable, ToXContentFragment {

    private BoundTransportAddress address;
    private Map<String, BoundTransportAddress> profileAddresses;

    public TransportInfo(BoundTransportAddress address, @Nullable Map<String, BoundTransportAddress> profileAddresses) {
        this.address = address;
        this.profileAddresses = profileAddresses;
    }

    public TransportInfo(StreamInput in) throws IOException {
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

    static final class Fields {
        static final String TRANSPORT = "transport";
        static final String BOUND_ADDRESS = "bound_address";
        static final String PUBLISH_ADDRESS = "publish_address";
        static final String PROFILES = "profiles";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.TRANSPORT);
        builder.array(Fields.BOUND_ADDRESS, (Object[]) address.boundAddresses());
        builder.field(Fields.PUBLISH_ADDRESS, address.publishAddress().toString());
        builder.startObject(Fields.PROFILES);
        if (profileAddresses != null && profileAddresses.size() > 0) {
            for (Map.Entry<String, BoundTransportAddress> entry : profileAddresses.entrySet()) {
                builder.startObject(entry.getKey());
                builder.array(Fields.BOUND_ADDRESS, (Object[]) entry.getValue().boundAddresses());
                builder.field(Fields.PUBLISH_ADDRESS, entry.getValue().publishAddress().toString());
                builder.endObject();
            }
        }
        builder.endObject();
        builder.endObject();
        return builder;
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
