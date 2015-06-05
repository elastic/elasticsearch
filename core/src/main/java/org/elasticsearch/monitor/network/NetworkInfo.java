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

package org.elasticsearch.monitor.network;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.io.Serializable;

/**
 *
 */
public class NetworkInfo implements Streamable, Serializable, ToXContent {

    public static final Interface NA_INTERFACE = new Interface();

    long refreshInterval;

    Interface primary = NA_INTERFACE;

    public long refreshInterval() {
        return this.refreshInterval;
    }

    public long getRefreshInterval() {
        return this.refreshInterval;
    }

    public Interface primaryInterface() {
        return primary;
    }

    public Interface getPrimaryInterface() {
        return primaryInterface();
    }

    static final class Fields {
        static final XContentBuilderString NETWORK = new XContentBuilderString("network");
        static final XContentBuilderString REFRESH_INTERVAL = new XContentBuilderString("refresh_interval");
        static final XContentBuilderString REFRESH_INTERVAL_IN_MILLIS = new XContentBuilderString("refresh_interval_in_millis");
        static final XContentBuilderString PRIMARY_INTERFACE = new XContentBuilderString("primary_interface");
        static final XContentBuilderString ADDRESS = new XContentBuilderString("address");
        static final XContentBuilderString NAME = new XContentBuilderString("name");
        static final XContentBuilderString MAC_ADDRESS = new XContentBuilderString("mac_address");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.NETWORK);
        builder.timeValueField(Fields.REFRESH_INTERVAL_IN_MILLIS, Fields.REFRESH_INTERVAL, refreshInterval);
        if (primary != NA_INTERFACE) {
            builder.startObject(Fields.PRIMARY_INTERFACE);
            builder.field(Fields.ADDRESS, primary.address());
            builder.field(Fields.NAME, primary.name());
            builder.field(Fields.MAC_ADDRESS, primary.macAddress());
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public static NetworkInfo readNetworkInfo(StreamInput in) throws IOException {
        NetworkInfo info = new NetworkInfo();
        info.readFrom(in);
        return info;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        refreshInterval = in.readLong();
        primary = Interface.readNetworkInterface(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(refreshInterval);
        primary.writeTo(out);
    }

    public static class Interface implements Streamable, Serializable {

        private String name = "";
        private String address = "";
        private String macAddress = "";

        private Interface() {
        }

        public Interface(String name, String address, String macAddress) {
            this.name = name;
            this.address = address;
            this.macAddress = macAddress;
        }

        public String name() {
            return name;
        }

        public String getName() {
            return name();
        }

        public String address() {
            return address;
        }

        public String getAddress() {
            return address();
        }

        public String macAddress() {
            return macAddress;
        }

        public String getMacAddress() {
            return macAddress();
        }

        public static Interface readNetworkInterface(StreamInput in) throws IOException {
            Interface inf = new Interface();
            inf.readFrom(in);
            return inf;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readString();
            address = in.readString();
            macAddress = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(address);
            out.writeString(macAddress);
        }

    }
}
