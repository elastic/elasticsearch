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

package org.elasticsearch.unconfigured_node_name;

import org.elasticsearch.common.logging.JsonLogsIntegTestCase;
import org.hamcrest.Matcher;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;

import static org.hamcrest.Matchers.equalTo;

public class JsonLogsFormatAndParseIT extends JsonLogsIntegTestCase {
    private static final String OS_NAME = System.getProperty("os.name");
    private static final boolean WINDOWS = OS_NAME.startsWith("Windows");

    // These match the values defined in org.elasticsearch.gradle.testclusters.ElasticsearchNode
    private static final String COMPUTERNAME = "WindowsComputername";
    private static final String HOSTNAME = "LinuxDarwinHostname";

    @Override
    protected Matcher<String> nodeNameMatcher() {
        if (WINDOWS) {
            return equalTo(COMPUTERNAME);
        }
        return equalTo(HOSTNAME);
    }

    @Override
    protected BufferedReader openReader(Path logFile) {
        return AccessController.doPrivileged((PrivilegedAction<BufferedReader>) () -> {
            try {
                return Files.newBufferedReader(logFile, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
