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

package org.elasticsearch.http;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class HttpInfo implements Writeable, ToXContentFragment {

    private final BoundTransportAddress address;
    private final long maxContentLength;

    public HttpInfo(StreamInput in) throws IOException {
        address = BoundTransportAddress.readBoundTransportAddress(in);
        maxContentLength = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        address.writeTo(out);
        out.writeLong(maxContentLength);
    }

    public HttpInfo(BoundTransportAddress address, long maxContentLength) {
        this.address = address;
        this.maxContentLength = maxContentLength;
    }

    static final class Fields {
        static final String HTTP = "http";
        static final String BOUND_ADDRESS = "bound_address";
        static final String PUBLISH_ADDRESS = "publish_address";
        static final String MAX_CONTENT_LENGTH = "max_content_length";
        static final String MAX_CONTENT_LENGTH_IN_BYTES = "max_content_length_in_bytes";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.HTTP);
        builder.array(Fields.BOUND_ADDRESS, (Object[]) address.boundAddresses());
        builder.field(Fields.PUBLISH_ADDRESS, address.publishAddress().toString());
        builder.byteSizeField(Fields.MAX_CONTENT_LENGTH_IN_BYTES, Fields.MAX_CONTENT_LENGTH, maxContentLength);
        builder.endObject();
        return builder;
    }

    public BoundTransportAddress address() {
        return address;
    }

    public BoundTransportAddress getAddress() {
        return address();
    }

    public ByteSizeValue maxContentLength() {
        return new ByteSizeValue(maxContentLength);
    }

    public ByteSizeValue getMaxContentLength() {
        return maxContentLength();
    }
}
