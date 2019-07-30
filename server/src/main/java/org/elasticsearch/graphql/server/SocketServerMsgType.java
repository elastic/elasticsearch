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

package org.elasticsearch.graphql.server;

public enum SocketServerMsgType {

    CONNECTION_ERROR("GQL_CONNECTION_ERROR"),
    CONNECTION_ACK("GQL_CONNECTION_ACK"),
    CONNECTION_KEEP_ALIVE("GQL_CONNECTION_KEEP_ALIVE"),
    DATA("GQL_DATA"),
    ERROR("GQL_ERROR"),
    COMPLETE("GQL_COMPLETE");

    private String type;

    SocketServerMsgType(String type) {
        this.type = type;
    }

    public String value() {
        return type;
    }
}
