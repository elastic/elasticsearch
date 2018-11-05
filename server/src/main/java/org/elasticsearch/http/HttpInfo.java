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

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.common.Booleans.parseBoolean;

public class HttpInfo implements Writeable, ToXContentFragment {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(HttpInfo.class));

    /** Whether to add hostname to publish host field when serializing. */
    private static final boolean CNAME_IN_PUBLISH_HOST =
        parseBoolean(System.getProperty("es.http.cname_in_publish_address"), true);

    private final BoundTransportAddress address;
    private final long maxContentLength;
    private final boolean cnameInPublishHost;

    public HttpInfo(StreamInput in) throws IOException {
        this(BoundTransportAddress.readBoundTransportAddress(in), in.readLong(), CNAME_IN_PUBLISH_HOST);
    }

    public HttpInfo(BoundTransportAddress address, long maxContentLength) {
        this(address, maxContentLength, CNAME_IN_PUBLISH_HOST);
    }

    HttpInfo(BoundTransportAddress address, long maxContentLength, boolean cnameInPublishHost) {
        this.address = address;
        this.maxContentLength = maxContentLength;
        this.cnameInPublishHost = cnameInPublishHost;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        address.writeTo(out);
        out.writeLong(maxContentLength);
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
        TransportAddress publishAddress = address.publishAddress();
        String publishAddressString = publishAddress.toString();
        String hostString = publishAddress.address().getHostString();
        if (InetAddresses.isInetAddress(hostString) == false) {
            if (cnameInPublishHost) {
                publishAddressString = hostString + '/' + publishAddress.toString();
            } else {
                deprecationLogger.deprecated(
                    "[http.publish_host] was printed as [ip:port] instead of [hostname/ip:port]. "
                        + "This format is deprecated and will change to [hostname/ip:port] in a future version. "
                        + "Use -Des.http.cname_in_publish_address=true to enforce non-deprecated formatting."
                );
            }
        }
        builder.field(Fields.PUBLISH_ADDRESS, publishAddressString);
        builder.humanReadableField(Fields.MAX_CONTENT_LENGTH_IN_BYTES, Fields.MAX_CONTENT_LENGTH, maxContentLength());
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
