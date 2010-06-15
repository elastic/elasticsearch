/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.monitor.network;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.builder.XContentBuilder;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author kimchy (shay.banon)
 */
public class NetworkInfo implements Streamable, Serializable, ToXContent {

    public static final Interface NA_INTERFACE = new Interface();

    Interface primary = NA_INTERFACE;

    public Interface primaryInterface() {
        return primary;
    }

    public Interface getPrimaryInterface() {
        return primaryInterface();
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("network");
        if (primary != NA_INTERFACE) {
            builder.startObject("primary_interface");
            builder.field("address", primary.address());
            builder.field("name", primary.name());
            builder.field("mac_address", primary.macAddress());
            builder.endObject();
        }
        builder.endObject();
    }

    public static NetworkInfo readNetworkInfo(StreamInput in) throws IOException {
        NetworkInfo info = new NetworkInfo();
        info.readFrom(in);
        return info;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        primary = Interface.readNetworkInterface(in);
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
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

        @Override public void readFrom(StreamInput in) throws IOException {
            name = in.readUTF();
            address = in.readUTF();
            macAddress = in.readUTF();
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            out.writeUTF(name);
            out.writeUTF(address);
            out.writeUTF(macAddress);
        }

    }
}
