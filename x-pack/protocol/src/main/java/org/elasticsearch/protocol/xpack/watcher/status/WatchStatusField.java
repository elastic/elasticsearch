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

public interface WatchStatusField {
    ParseField STATE = new ParseField("state");
    ParseField ACTIVE = new ParseField("active");
    ParseField TIMESTAMP = new ParseField("timestamp");
    ParseField LAST_CHECKED = new ParseField("last_checked");
    ParseField LAST_MET_CONDITION = new ParseField("last_met_condition");
    ParseField ACTIONS = new ParseField("actions");
    ParseField VERSION = new ParseField("version");
    ParseField EXECUTION_STATE = new ParseField("execution_state");
    ParseField HEADERS = new ParseField("headers");
}
