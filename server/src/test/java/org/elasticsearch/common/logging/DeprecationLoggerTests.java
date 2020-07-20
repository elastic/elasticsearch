/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class DeprecationLoggerTests extends ESTestCase {

    public void testMultipleSlowLoggersUseSingleLog4jLogger() {
        org.apache.logging.log4j.core.LoggerContext context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);

        DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(DeprecationLoggerTests.class);
        int numberOfLoggersBefore = context.getLoggers().size();

        class LoggerTest{
        }
        DeprecationLogger deprecationLogger2 = DeprecationLogger.getLogger(LoggerTest.class);

        context = (org.apache.logging.log4j.core.LoggerContext) LogManager.getContext(false);
        int numberOfLoggersAfter = context.getLoggers().size();

        assertThat(numberOfLoggersAfter, equalTo(numberOfLoggersBefore+1));
    }
}
