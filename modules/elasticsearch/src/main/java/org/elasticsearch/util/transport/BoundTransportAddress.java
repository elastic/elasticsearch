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

package org.elasticsearch.util.transport;

/**
 * A bounded transport address is a tuple of two {@link TransportAddress}, one that represents
 * the address the transport is bounded on, the the published one represents the one clients should
 * communicate on.
 *
 * @author kimchy (shay.banon)
 */
public class BoundTransportAddress {

    private final TransportAddress boundAddress;

    private final TransportAddress publishAddress;

    public BoundTransportAddress(TransportAddress boundAddress, TransportAddress publishAddress) {
        this.boundAddress = boundAddress;
        this.publishAddress = publishAddress;
    }

    public TransportAddress boundAddress() {
        return boundAddress;
    }

    public TransportAddress publishAddress() {
        return publishAddress;
    }

    @Override public String toString() {
        return "boundAddress [" + boundAddress + "], publishAddress [" + publishAddress + "]";
    }
}
