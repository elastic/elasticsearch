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

package org.elasticsearch.common.logging.jdk;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class JDKESLoggerTests extends ESTestCase {

    private ESLogger esTestLogger;
    private TestHandler testHandler;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        JdkESLoggerFactory esTestLoggerFactory = new JdkESLoggerFactory();
        esTestLogger = esTestLoggerFactory.newInstance("test");
        Logger testLogger = ((JdkESLogger) esTestLogger).logger();
        testLogger.setLevel(Level.FINEST);
        assertThat(testLogger.getLevel(), equalTo(Level.FINEST));
        testHandler = new TestHandler();
        testLogger.addHandler(testHandler);
    }

    @Test
    public void locationInfoTest() {
        esTestLogger.error("This is an error");
        esTestLogger.warn("This is a warning");
        esTestLogger.info("This is an info");
        esTestLogger.debug("This is a debug");
        esTestLogger.trace("This is a trace");
        List<LogRecord> records = testHandler.getEvents();
        assertThat(records, notNullValue());
        assertThat(records.size(), equalTo(5));
        LogRecord record = records.get(0);
        assertThat(record, notNullValue());
        assertThat(record.getLevel(), equalTo(Level.SEVERE));
        assertThat(record.getMessage(), equalTo("This is an error"));
        assertThat(record.getSourceClassName(), equalTo(JDKESLoggerTests.class.getCanonicalName()));
        assertThat(record.getSourceMethodName(), equalTo("locationInfoTest"));
        record = records.get(1);
        assertThat(record, notNullValue());
        assertThat(record.getLevel(), equalTo(Level.WARNING));
        assertThat(record.getMessage(), equalTo("This is a warning"));
        assertThat(record.getSourceClassName(), equalTo(JDKESLoggerTests.class.getCanonicalName()));
        assertThat(record.getSourceMethodName(), equalTo("locationInfoTest"));
        record = records.get(2);
        assertThat(record, notNullValue());
        assertThat(record.getLevel(), equalTo(Level.INFO));
        assertThat(record.getMessage(), equalTo("This is an info"));
        assertThat(record.getSourceClassName(), equalTo(JDKESLoggerTests.class.getCanonicalName()));
        assertThat(record.getSourceMethodName(), equalTo("locationInfoTest"));
        record = records.get(3);
        assertThat(record, notNullValue());
        assertThat(record.getLevel(), equalTo(Level.FINE));
        assertThat(record.getMessage(), equalTo("This is a debug"));
        assertThat(record.getSourceClassName(), equalTo(JDKESLoggerTests.class.getCanonicalName()));
        assertThat(record.getSourceMethodName(), equalTo("locationInfoTest"));
        record = records.get(4);
        assertThat(record, notNullValue());
        assertThat(record.getLevel(), equalTo(Level.FINEST));
        assertThat(record.getMessage(), equalTo("This is a trace"));
        assertThat(record.getSourceClassName(), equalTo(JDKESLoggerTests.class.getCanonicalName()));
        assertThat(record.getSourceMethodName(), equalTo("locationInfoTest"));
    }

    private static class TestHandler extends Handler {

        private List<LogRecord> records = new ArrayList<>();

        @Override
        public void close() {
        }

        public List<LogRecord> getEvents() {
            return records;
        }

        @Override
        public void publish(LogRecord record) {
            // Forces it to generate the location information
            record.getSourceClassName();
            records.add(record);
        }

        @Override
        public void flush() {
        }
    }
}
