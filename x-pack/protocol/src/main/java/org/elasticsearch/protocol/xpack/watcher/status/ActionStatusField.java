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
package org.elasticsearch.protocol.xpack.watcher.status;

import org.elasticsearch.common.ParseField;

public interface ActionStatusField {
    ParseField ACK_STATUS = new ParseField("ack");
    ParseField ACK_STATUS_STATE = new ParseField("state");

    ParseField LAST_EXECUTION = new ParseField("last_execution");
    ParseField LAST_SUCCESSFUL_EXECUTION = new ParseField("last_successful_execution");
    ParseField EXECUTION_SUCCESSFUL = new ParseField("successful");

    ParseField LAST_THROTTLE = new ParseField("last_throttle");

    ParseField TIMESTAMP = new ParseField("timestamp");
    ParseField REASON = new ParseField("reason");
}
