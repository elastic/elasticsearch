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

package org.elasticsearch.cli;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;

/**
 * Holder class for method to configure logging without Elasticsearch configuration files for use in CLI tools that will not read such
 * files.
 */
public final class CommandLoggingConfigurator {

    /**
     * Configures logging without Elasticsearch configuration files based on the system property "es.logger.level" only. As such, any
     * logging will be written to the console.
     */
    public static void configureLoggingWithoutConfig() {
        // initialize default for es.logger.level because we will not read the log4j2.properties
        final String loggerLevel = System.getProperty("es.logger.level", Level.INFO.name());
        final Settings settings = Settings.builder().put("logger.level", loggerLevel).build();
        LogConfigurator.configureWithoutConfig(settings);
    }

}
