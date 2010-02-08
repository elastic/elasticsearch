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

package org.elasticsearch.transport;

import org.elasticsearch.ElasticSearchWrapperException;
import org.elasticsearch.util.transport.TransportAddress;

/**
 * @author kimchy (Shay Banon)
 */
public class RemoteTransportException extends TransportException implements ElasticSearchWrapperException {

    private TransportAddress address;

    private String action;

    public RemoteTransportException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public RemoteTransportException(String name, TransportAddress address, String action, Throwable cause) {
        super(buildMessage(name, address, action), cause);
        this.address = address;
        this.action = action;
    }

    public TransportAddress address() {
        return address;
    }

    public String action() {
        return action;
    }

    @Override public Throwable fillInStackTrace() {
        // no need for stack trace here, we always have cause
        return null;
    }

    private static String buildMessage(String name, TransportAddress address, String action) {
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
        return sb.toString();
    }
}
