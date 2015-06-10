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

package org.elasticsearch.common.settings.loader;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class YamlSettingsLoaderTests extends ElasticsearchTestCase {

    @Test
    public void testSimpleYamlSettings() throws Exception {
        Settings settings = settingsBuilder()
                .loadFromClasspath("org/elasticsearch/common/settings/loader/test-settings.yml")
                .build();

        assertThat(settings.get("test1.value1"), equalTo("value1"));
        assertThat(settings.get("test1.test2.value2"), equalTo("value2"));
        assertThat(settings.getAsInt("test1.test2.value3", -1), equalTo(2));

        // check array
        assertThat(settings.get("test1.test3.0"), equalTo("test3-1"));
        assertThat(settings.get("test1.test3.1"), equalTo("test3-2"));
        assertThat(settings.getAsArray("test1.test3").length, equalTo(2));
        assertThat(settings.getAsArray("test1.test3")[0], equalTo("test3-1"));
        assertThat(settings.getAsArray("test1.test3")[1], equalTo("test3-2"));
    }
    
    @Test
    @TestLogging("loader:WARN") // To ensure that we log cluster state events on WARN level
    public void testYamlSettingsNoFile() throws Exception {
        String invalidResourceName = "org/elasticsearch/common/settings/loader/no-test-settings.yml";
        MockAppender mockAppender = new MockAppender();
        Logger rootLogger = Logger.getRootLogger();
        Level savedLevel = rootLogger.getLevel();
        rootLogger.addAppender(mockAppender);
        rootLogger.setLevel(Level.WARN);
        rootLogger.addAppender(mockAppender);
        try {
            Settings defaultSettings = settingsBuilder()
                .loadFromClasspath(invalidResourceName)
                .build();
            assertTrue(mockAppender.sawLoadFailed);
            assertFalse(defaultSettings == null);
        } finally {
            rootLogger.removeAppender(mockAppender);
            rootLogger.setLevel(savedLevel);
        }
    }
    
    private static class MockAppender extends AppenderSkeleton {
        public boolean sawLoadFailed = false;

        @Override
        protected void append(LoggingEvent event) {
            String message = event.getMessage().toString();
            if (event.getLevel() == Level.WARN && message.contains("Failed to load settings from [")) {
                sawLoadFailed = true;
            }
        }

        @Override
        public boolean requiresLayout() {
            return false;
        }

        @Override
        public void close() {
        }
    }
    
}