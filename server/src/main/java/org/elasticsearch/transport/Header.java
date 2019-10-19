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

import org.elasticsearch.Version;

public class Header {

    private final int networkMessageSize;
    private final Version version;
    private final long requestId;
    private final byte status;

    Header(int networkMessageSize, long requestId, byte status, Version version) {
        this.networkMessageSize = networkMessageSize;
        this.version = version;
        this.requestId = requestId;
        this.status = status;
    }

    public int getNetworkMessageSize() {
        return networkMessageSize;
    }

    Version getVersion() {
        return version;
    }

    long getRequestId() {
        return requestId;
    }

    byte getStatus() {
        return status;
    }

    boolean isRequest() {
        return TransportStatus.isRequest(status);
    }

    boolean isResponse() {
        return TransportStatus.isRequest(status) == false;
    }

    boolean isError() {
        return TransportStatus.isError(status);
    }

    boolean isHandshake() {
        return TransportStatus.isHandshake(status);
    }

    boolean isCompress() {
        return TransportStatus.isCompress(status);
    }

    boolean isCompressed() {
        return TransportStatus.isCompress(status);
    }
}
