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

import org.elasticsearch.common.transport.TransportAddress;

/**
 * An action invocation failure.
 *
 *
 */
public class ActionTransportException extends TransportException {

    private TransportAddress address;

    private String action;

    public ActionTransportException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public ActionTransportException(String name, TransportAddress address, String action, Throwable cause) {
        super(buildMessage(name, address, action, null), cause);
        this.address = address;
        this.action = action;
    }

    public ActionTransportException(String name, TransportAddress address, String action, String msg, Throwable cause) {
        super(buildMessage(name, address, action, msg), cause);
        this.address = address;
        this.action = action;
    }

    /**
     * The target address to invoke the action on.
     */
    public TransportAddress address() {
        return address;
    }

    /**
     * The action to invoke.
     */
    public String action() {
        return action;
    }

    private static String buildMessage(String name, TransportAddress address, String action, String msg) {
        StringBuilder sb = new StringBuilder();
        if (name != null) {
            sb.append('[').append(name).append(']');
        }
        if (address != null) {
            sb.append('[').append(address).append(']');
        }
        if (action != null) {
            sb.append('[').append(action).append(']');
        }
        if (msg != null) {
            sb.append(" ").append(msg);
        }
        return sb.toString();
    }
}
