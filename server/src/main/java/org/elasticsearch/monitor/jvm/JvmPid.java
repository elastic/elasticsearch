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

package org.elasticsearch.monitor.jvm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.message.ParameterizedMessage;

import java.lang.management.ManagementFactory;

class JvmPid {

    private static final long PID;

    static long getPid() {
        return PID;
    }

    static {
        PID = initializePid();
    }

    private static long initializePid() {
        final String name = ManagementFactory.getRuntimeMXBean().getName();
        try {
            return Long.parseLong(name.split("@")[0]);
        } catch (final NumberFormatException e) {
            LogManager.getLogger(JvmPid.class).debug(new ParameterizedMessage("failed parsing PID from [{}]", name), e);
            return -1;
        }
    }

}
