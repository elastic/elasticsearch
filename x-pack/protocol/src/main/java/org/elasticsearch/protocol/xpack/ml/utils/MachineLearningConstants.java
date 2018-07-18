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
package org.elasticsearch.protocol.xpack.ml.utils;

import org.elasticsearch.common.unit.TimeValue;

public final class MachineLearningConstants {

    public static final TimeValue STATE_PERSIST_RESTORE_TIMEOUT = TimeValue.timeValueMinutes(30);

    /**
     * Value must match api::CAnomalyDetector::CONTROL_FIELD_NAME in the C++
     * code.
     */
    public static final String CONTROL_FIELD_NAME = ".";

    /**
     * Value must match api::CBaseTokenListDataTyper::PRETOKENISED_TOKEN_FIELD in the C++
     * code.
     */
    public static final String PRETOKENISED_TOKEN_FIELD = "...";

    private MachineLearningConstants() {
    }
}
