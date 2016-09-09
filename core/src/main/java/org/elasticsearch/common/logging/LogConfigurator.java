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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.AbstractConfiguration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;
import org.apache.logging.log4j.core.config.composite.CompositeConfiguration;
import org.apache.logging.log4j.core.config.properties.PropertiesConfiguration;
import org.apache.logging.log4j.core.config.properties.PropertiesConfigurationFactory;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LogConfigurator {

    public static void configure(final Environment environment, final boolean resolveConfig) throws IOException {
        final Settings settings = environment.settings();

        setLogConfigurationSystemProperty(environment, settings);

        // we initialize the status logger immediately otherwise Log4j will complain when we try to get the context
        final ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();
        builder.setStatusLevel(Level.ERROR);
        Configurator.initialize(builder.build());

        final LoggerContext context = (LoggerContext) LogManager.getContext(false);

        if (resolveConfig) {
            final List<AbstractConfiguration> configurations = new ArrayList<>();
            final PropertiesConfigurationFactory factory = new PropertiesConfigurationFactory();
            final Set<FileVisitOption> options = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
            Files.walkFileTree(environment.configFile(), options, Integer.MAX_VALUE, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (file.getFileName().toString().equals("log4j2.properties")) {
                        configurations.add((PropertiesConfiguration) factory.getConfiguration(file.toString(), file.toUri()));
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
            context.start(new CompositeConfiguration(configurations));
        }

        if (ESLoggerFactory.LOG_DEFAULT_LEVEL_SETTING.exists(settings)) {
            Loggers.setLevel(ESLoggerFactory.getRootLogger(), ESLoggerFactory.LOG_DEFAULT_LEVEL_SETTING.get(settings));
        }

        final Map<String, String> levels = settings.filter(ESLoggerFactory.LOG_LEVEL_SETTING::match).getAsMap();
        for (String key : levels.keySet()) {
            final Level level = ESLoggerFactory.LOG_LEVEL_SETTING.getConcreteSetting(key).get(settings);
            Loggers.setLevel(Loggers.getLogger(key.substring("logger.".length())), level);
        }
    }

    @SuppressForbidden(reason = "sets system property for logging configuration")
    private static void setLogConfigurationSystemProperty(final Environment environment, final Settings settings) {
        System.setProperty("es.logs", environment.logsFile().resolve(ClusterName.CLUSTER_NAME_SETTING.get(settings).value()).toString());
    }

}
