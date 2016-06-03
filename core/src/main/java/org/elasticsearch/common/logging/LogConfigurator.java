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

import org.apache.log4j.Java9Hack;
import org.apache.log4j.PropertyConfigurator;
import org.apache.lucene.util.Constants;
import org.elasticsearch.ElasticsearchException;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.Strings.cleanPath;

/**
 * Configures log4j with a special set of replacements.
 */
public class LogConfigurator {

    static final List<String> ALLOWED_SUFFIXES = Arrays.asList(".yml", ".yaml", ".json", ".properties");

    private static final Map<String, String> REPLACEMENTS;
    static {
        Map<String, String> replacements = new HashMap<>();
        // Appenders
        replacements.put("async", "org.apache.log4j.AsyncAppender");
        replacements.put("console", ConsoleAppender.class.getName());
        replacements.put("dailyRollingFile", "org.apache.log4j.DailyRollingFileAppender");
        replacements.put("externallyRolledFile", "org.apache.log4j.ExternallyRolledFileAppender");
        replacements.put("extrasRollingFile", "org.apache.log4j.rolling.RollingFileAppender");
        replacements.put("file", "org.apache.log4j.FileAppender");
        replacements.put("jdbc", "org.apache.log4j.jdbc.JDBCAppender");
        replacements.put("jms", "org.apache.log4j.net.JMSAppender");
        replacements.put("lf5", "org.apache.log4j.lf5.LF5Appender");
        replacements.put("ntevent", "org.apache.log4j.nt.NTEventLogAppender");
        replacements.put("null", "org.apache.log4j.NullAppender");
        replacements.put("rollingFile", "org.apache.log4j.RollingFileAppender");
        replacements.put("smtp", "org.apache.log4j.net.SMTPAppender");
        replacements.put("socket", "org.apache.log4j.net.SocketAppender");
        replacements.put("socketHub", "org.apache.log4j.net.SocketHubAppender");
        replacements.put("syslog", "org.apache.log4j.net.SyslogAppender");
        replacements.put("telnet", "org.apache.log4j.net.TelnetAppender");
        replacements.put("terminal", TerminalAppender.class.getName());

        // Policies
        replacements.put("timeBased", "org.apache.log4j.rolling.TimeBasedRollingPolicy");
        replacements.put("sizeBased", "org.apache.log4j.rolling.SizeBasedTriggeringPolicy");

        // Layouts
        replacements.put("simple", "org.apache.log4j.SimpleLayout");
        replacements.put("html", "org.apache.log4j.HTMLLayout");
        replacements.put("pattern", "org.apache.log4j.PatternLayout");
        replacements.put("consolePattern", "org.apache.log4j.PatternLayout");
        replacements.put("enhancedPattern", "org.apache.log4j.EnhancedPatternLayout");
        replacements.put("ttcc", "org.apache.log4j.TTCCLayout");
        replacements.put("xml", "org.apache.log4j.XMLLayout");
        REPLACEMENTS = unmodifiableMap(replacements);

        if (Constants.JRE_IS_MINIMUM_JAVA9) {
            Java9Hack.fixLog4j();
        }
    }

    private static boolean loaded;

    /**
     * Consolidates settings and converts them into actual log4j settings, then initializes loggers and appenders.
     *  @param settings      custom settings that should be applied
     * @param resolveConfig controls whether the logging conf file should be read too or not.
     */
    public static void configure(Settings settings, boolean resolveConfig) {
        if (loaded) {
            return;
        }
        loaded = true;
        // TODO: this is partly a copy of InternalSettingsPreparer...we should pass in Environment and not do all this...
        Environment environment = new Environment(settings);

        Settings.Builder settingsBuilder = Settings.builder();
        if (resolveConfig) {
            resolveConfig(environment, settingsBuilder);
        }

        // add custom settings after config was added so that they are not overwritten by config
        settingsBuilder.put(settings);
        settingsBuilder.replacePropertyPlaceholders();
        Properties props = new Properties();
        for (Map.Entry<String, String> entry : settingsBuilder.build().getAsMap().entrySet()) {
            String key = "log4j." + entry.getKey();
            String value = entry.getValue();
            value = REPLACEMENTS.getOrDefault(value, value);
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
            Set<FileVisitOption> options = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
            Files.walkFileTree(env.configFile(), options, Integer.MAX_VALUE, new SimpleFileVisitor<Path>() {
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
        } catch (IOException | SettingsException | NoClassDefFoundError e) {
            // ignore
        }
    }
}
