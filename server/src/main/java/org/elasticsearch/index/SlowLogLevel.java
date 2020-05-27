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
package org.elasticsearch.index;

import java.util.Locale;

public enum SlowLogLevel {
    WARN(0), //most specific - little logging
    INFO(1),
    DEBUG(2),
    TRACE(3); //least specific - lots of logging

    private final int precedence;

    SlowLogLevel(int precedence) {
        this.precedence = precedence;
    }

    public static SlowLogLevel parse(String level) {
        return valueOf(level.toUpperCase(Locale.ROOT));
    }

    boolean isLevelEnabledFor(SlowLogLevel levelToBeUsed) {
        // info is less specific then warn
        return this.precedence >= levelToBeUsed.precedence;
    }
}
