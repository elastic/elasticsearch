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

package org.elasticsearch.xpack.watcher;

import java.time.Clock;

/**
 * A wrapper around {@link java.time.Clock} to provide a concrete type for Guice injection.
 *
 * This class is temporary until {@link java.time.Clock} can be passed to action constructors
 * directly, or the actions can be rewritten to be unit tested with the clock overriden
 * just for unit tests instead of via Node construction.
 */
public final class ClockHolder {
    public final Clock clock;

    public ClockHolder(Clock clock) {
        this.clock = clock;
    }
}
