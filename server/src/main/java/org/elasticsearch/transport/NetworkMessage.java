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
import org.elasticsearch.common.util.concurrent.ThreadContext;

public abstract class NetworkMessage {

    protected final Version version;
    protected final ThreadContext threadContext;
    protected final ThreadContext.StoredContext storedContext;
    protected final long requestId;
    protected byte status;

    NetworkMessage(ThreadContext threadContext, Version version, byte status, long requestId) {
        this.threadContext = threadContext;
        storedContext = threadContext.stashContext();
        this.version = version;
        this.requestId = requestId;
        this.status = status;
    }

    boolean isCompress() {
        return TransportStatus.isCompress(status);
    }

    boolean isResponse() {
        return TransportStatus.isRequest(status) == false;
    }

    boolean isRequest() {
        return TransportStatus.isRequest(status);
    }

    boolean isHandshake() {
        return TransportStatus.isHandshake(status);
    }

    boolean isError() {
        return TransportStatus.isError(status);
    }
}
