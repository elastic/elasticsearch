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

package org.elasticsearch.common.logging;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.elasticsearch.test.ESIntegTestCase;

/**
 * Converts {@code %test_thread_info} in log4j patterns into information
 * based on the loggin thread's name. If that thread is part of an
 * {@link ESIntegTestCase} then this information is the node name.
 */
@Plugin(category = PatternConverter.CATEGORY, name = "TestInfoPatternConverter")
@ConverterKeys({"test_thread_info"})
public class TestThreadInfoPatternConverter extends LogEventPatternConverter {
    /**
     * Called by log4j2 to initialize this converter.
     */
    public static TestThreadInfoPatternConverter newInstance(final String[] options) {
        if (options.length > 0) {
            throw new IllegalArgumentException("no options supported but options provided: "
                    + Arrays.toString(options));
        }
        return new TestThreadInfoPatternConverter();
    }

    private TestThreadInfoPatternConverter() {
        super("TestInfo", "test_thread_info");
    }

    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        toAppendTo.append(threadInfo(event.getThreadName()));
        if (event.getContextData().isEmpty() == false) {
            toAppendTo.append(event.getContextData());
        }
    }

    private static final Pattern ELASTICSEARCH_THREAD_NAME_PATTERN =
            Pattern.compile("elasticsearch\\[(.+)\\]\\[.+\\].+");
    private static final Pattern TEST_THREAD_NAME_PATTERN =
            Pattern.compile("TEST-.+\\.(.+)-seed#\\[.+\\]");
    private static final Pattern TEST_SUITE_INIT_THREAD_NAME_PATTERN =
            Pattern.compile("SUITE-.+-worker");
    private static final Pattern NOT_YET_NAMED_NODE_THREAD_NAME_PATTERN =
            Pattern.compile("test_SUITE-CHILD_VM.+cluster\\[T#(.+)\\]");

    static String threadInfo(String threadName) {
        Matcher m = ELASTICSEARCH_THREAD_NAME_PATTERN.matcher(threadName);
        if (m.matches()) {
            // Thread looks like a node thread so use the node name
            return m.group(1);
        }
        m = TEST_THREAD_NAME_PATTERN.matcher(threadName);
        if (m.matches()) {
            /*
             * Thread looks like a test thread so use the test method name.
             * It'd be pretty reasonable not to use a prefix at all here but
             * the logger layout pretty much expects one and the test method
             * can be pretty nice to have around anyway.
             */
            return m.group(1);
        }
        m = TEST_SUITE_INIT_THREAD_NAME_PATTERN.matcher(threadName);
        if (m.matches()) {
            /*
             * Thread looks like test suite initialization or tead down and
             * we don't have any more information to give. Like above, we
             * could spit out nothing here but the logger layout expect
             * something and it *is* nice to know what lines come from test
             * teardown and initialization.
             */
            return "suite";
        }
        m = NOT_YET_NAMED_NODE_THREAD_NAME_PATTERN.matcher(threadName);
        if (m.matches()) {
            /*
             * These are as yet unnamed integ test nodes. I'd prefer to log
             * the node name but I don't have it yet.
             */
            return "integ_" + m.group(1) + "";
        }
        /*
         * These are uncategorized threads. We log the entire thread name in
         * case it is useful. We wrap it in `[]` so you tell that it is a
         * thread name rather than a node name or something.
         */
        return "[" + threadName + "]";
    }
}
