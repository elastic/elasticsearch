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

package org.apache.log4j;

import org.apache.log4j.helpers.ThreadLocalMap;

/**
 * Log4j 1.2 MDC breaks because it parses java.version incorrectly (does not handle new java9 versioning).
 *
 * This hack fixes up the pkg private members as if it had detected the java version correctly.
 */
public class Java9Hack {

    public static void fixLog4j() {
        if (MDC.mdc.tlm == null) {
            MDC.mdc.java1 = false;
            MDC.mdc.tlm = new ThreadLocalMap();
        }
    }
}
