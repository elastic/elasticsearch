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

package org.elasticsearch.common.logging.log4j;

import com.google.common.collect.ImmutableMap;
import org.apache.log4j.PropertyConfigurator;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.elasticsearch.common.Strings.cleanPath;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;

/**
 *
 */
public class LogConfigurator {

    static final List<String> ALLOWED_SUFFIXES = Arrays.asList(".yml", ".yaml", ".json", ".properties");

    private static boolean loaded;

    private static ImmutableMap<String, String> replacements = new MapBuilder<String, String>()
            .put("console", "org.elasticsearch.common.logging.log4j.ConsoleAppender")
            .put("async", "org.apache.log4j.AsyncAppender")
            .put("dailyRollingFile", "org.apache.log4j.DailyRollingFileAppender")
            .put("externallyRolledFile", "org.apache.log4j.ExternallyRolledFileAppender")
            .put("file", "org.apache.log4j.FileAppender")
            .put("jdbc", "org.apache.log4j.jdbc.JDBCAppender")
            .put("jms", "org.apache.log4j.net.JMSAppender")
            .put("lf5", "org.apache.log4j.lf5.LF5Appender")
            .put("ntevent", "org.apache.log4j.nt.NTEventLogAppender")
            .put("null", "org.apache.log4j.NullAppender")
            .put("rollingFile", "org.apache.log4j.RollingFileAppender")
            .put("extrasRollingFile", "org.apache.log4j.rolling.RollingFileAppender")
            .put("smtp", "org.apache.log4j.net.SMTPAppender")
            .put("socket", "org.apache.log4j.net.SocketAppender")
            .put("socketHub", "org.apache.log4j.net.SocketHubAppender")
            .put("syslog", "org.apache.log4j.net.SyslogAppender")
            .put("telnet", "org.apache.log4j.net.TelnetAppender")
                    // policies
            .put("timeBased", "org.apache.log4j.rolling.TimeBasedRollingPolicy")
            .put("sizeBased", "org.apache.log4j.rolling.SizeBasedTriggeringPolicy")
                    // layouts
            .put("simple", "org.apache.log4j.SimpleLayout")
            .put("html", "org.apache.log4j.HTMLLayout")
            .put("pattern", "org.apache.log4j.PatternLayout")
            .put("consolePattern", "org.apache.log4j.PatternLayout")
            .put("enhancedPattern", "org.apache.log4j.EnhancedPatternLayout")
            .put("ttcc", "org.apache.log4j.TTCCLayout")
            .put("xml", "org.apache.log4j.XMLLayout")
            .immutableMap();

    public static void configure(Settings settings) {
        if (loaded) {
            return;
        }
        loaded = true;
        // TODO: this is partly a copy of InternalSettingsPreparer...we should pass in Environment and not do all this...
        Environment environment = new Environment(settings);
        Settings.Builder settingsBuilder = settingsBuilder().put(settings);
        resolveConfig(environment, settingsBuilder);
        settingsBuilder
                .putProperties("elasticsearch.", System.getProperties())
                .putProperties("es.", System.getProperties())
                .replacePropertyPlaceholders();
        Properties props = new Properties();
        for (Map.Entry<String, String> entry : settingsBuilder.build().getAsMap().entrySet()) {
            String key = "log4j." + entry.getKey();
            String value = entry.getValue();
            if (replacements.containsKey(value)) {
                value = replacements.get(value);
            }
            if (key.endsWith(".value")) {
                props.setProperty(key.substring(0, key.length() - ".value".length()), value);
            } else if (key.endsWith(".type")) {
                props.setProperty(key.substring(0, key.length() - ".type".length()), value);
            } else {
                props.setProperty(key, value);
            }
        }
        // ensure explicit path to logs dir exists
        props.setProperty("log4j.path.logs", cleanPath(environment.logsFile().toAbsolutePath().toString()));
        PropertyConfigurator.configure(props);
    }

    /**
     * sets the loaded flag to false so that logging configuration can be
     * overridden. Should only be used in tests.
     */
    static void reset() {
        loaded = false;
    }

    static void resolveConfig(Environment env, final Settings.Builder settingsBuilder) {

        try {
            Files.walkFileTree(env.configFile(), EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    String fileName = file.getFileName().toString();
                    if (fileName.startsWith("logging.")) {
                        for (String allowedSuffix : ALLOWED_SUFFIXES) {
                            if (fileName.endsWith(allowedSuffix)) {
                                loadConfig(file, settingsBuilder);
                                break;
                            }
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException ioe) {
            throw new ElasticsearchException("Failed to load logging configuration", ioe);
        }
    }

    static void loadConfig(Path file, Settings.Builder settingsBuilder) {
        try {
            settingsBuilder.loadFromPath(file);
        } catch (SettingsException | NoClassDefFoundError e) {
            // ignore
        }
    }
}
