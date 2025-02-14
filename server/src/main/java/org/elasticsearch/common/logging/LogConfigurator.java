/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.AbstractConfiguration;
import org.apache.logging.log4j.core.config.ConfigurationException;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;
import org.apache.logging.log4j.core.config.builder.impl.DefaultConfigurationBuilder;
import org.apache.logging.log4j.core.config.composite.CompositeConfiguration;
import org.apache.logging.log4j.core.config.plugins.util.PluginManager;
import org.apache.logging.log4j.core.config.properties.PropertiesConfiguration;
import org.apache.logging.log4j.core.config.properties.PropertiesConfigurationBuilder;
import org.apache.logging.log4j.core.config.properties.PropertiesConfigurationFactory;
import org.apache.logging.log4j.status.StatusConsoleListener;
import org.apache.logging.log4j.status.StatusData;
import org.apache.logging.log4j.status.StatusListener;
import org.apache.logging.log4j.status.StatusLogger;
import org.apache.logging.log4j.util.Unbox;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.logging.internal.LoggerFactoryImpl;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;
import org.elasticsearch.logging.internal.spi.LoggerFactory;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.StreamSupport;

public class LogConfigurator {

    /*
     * We want to detect situations where we touch logging before the configuration is loaded. If we do this, Log4j will status log an error
     * message at the error level. With this error listener, we can capture if this happens. More broadly, we can detect any error-level
     * status log message which likely indicates that something is broken. The listener is installed immediately on startup, and then when
     * we get around to configuring logging we check that no error-level log messages have been logged by the status logger. If they have we
     * fail startup and any such messages can be seen on the console.
     */
    private static final AtomicBoolean error = new AtomicBoolean();
    private static final StatusListener ERROR_LISTENER = new StatusConsoleListener(Level.ERROR) {
        @Override
        public void log(StatusData data) {
            error.set(true);
            super.log(data);
        }
    };

    private static Appender consoleAppender;

    /**
     * Registers a listener for status logger errors. This listener should be registered as early as possible to ensure that no errors are
     * logged by the status logger before logging is configured.
     */
    public static void registerErrorListener() {
        error.set(false);
        StatusLogger.getLogger().registerListener(ERROR_LISTENER);
    }

    /**
     * Configure logging without reading a log4j2.properties file, effectively configuring the
     * status logger and all loggers to the console.
     *
     * @param settings for configuring logger.level and individual loggers
     */
    public static void configureWithoutConfig(final Settings settings) {
        Objects.requireNonNull(settings);
        configureESLogging();
        // we initialize the status logger immediately otherwise Log4j will complain when we try to get the context
        configureStatusLogger();
        configureLoggerLevels(settings);
    }

    /**
     * Configure logging reading from any log4j2.properties found in the config directory and its
     * subdirectories from the specified environment. Will also configure logging to point the logs
     * directory from the specified environment.
     *
     * @param environment the environment for reading configs and the logs path
     * @param useConsole whether a console appender should exist
     * @throws IOException   if there is an issue readings any log4j2.properties in the config
     *                       directory
     */
    public static void configure(final Environment environment, boolean useConsole) throws IOException {
        Objects.requireNonNull(environment);
        try {
            // we are about to configure logging, check that the status logger did not log any error-level messages
            checkErrorListener();
        } finally {
            // whether or not the error listener check failed we can remove the listener now
            StatusLogger.getLogger().removeListener(ERROR_LISTENER);
        }
        configureESLogging();
        configure(environment.settings(), environment.configDir(), environment.logsDir(), useConsole);
        initializeStatics();
        // creates a permanent status logger that can watch for StatusLogger events and forward to a real logger
        configureStatusLoggerForwarder();
    }

    private static void configureStatusLoggerForwarder() {
        // the real logger is lazily retrieved here since logging won't yet be setup during clinit of this class
        var logger = LogManager.getLogger("StatusLogger");
        var listener = new StatusConsoleListener(Level.WARN) {
            @Override
            public void log(StatusData data) {
                logger.log(data.getLevel(), data.getMessage(), data.getThrowable());
                super.log(data);
            }
        };
        StatusLogger.getLogger().registerListener(listener);
    }

    public static void configureESLogging() {
        LoggerFactory.setInstance(new LoggerFactoryImpl());
    }

    /**
     * Load logging plugins so we can have {@code node_name} in the pattern.
     */
    public static void loadLog4jPlugins() {
        PluginManager.addPackage(LogConfigurator.class.getPackage().getName());
    }

    /**
     * Sets the node name. This is called before logging is configured if the
     * node name is set in elasticsearch.yml. Otherwise it is called as soon
     * as the node id is available.
     */
    public static void setNodeName(String nodeName) {
        NodeNamePatternConverter.setNodeName(nodeName);
    }

    // Some classes within log4j have static initializers that require security manager permissions.
    // Here we aggressively initialize those classes during logging configuration so that
    // actual logging calls at runtime do not trigger that initialization.
    private static void initializeStatics() {
        try {
            MethodHandles.publicLookup().ensureInitialized(Unbox.class);
        } catch (IllegalAccessException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static void checkErrorListener() {
        assert errorListenerIsRegistered() : "expected error listener to be registered";
        if (error.get()) {
            throw new IllegalStateException("status logger logged an error before logging was configured");
        }
    }

    private static boolean errorListenerIsRegistered() {
        return StreamSupport.stream(StatusLogger.getLogger().getListeners().spliterator(), false).anyMatch(l -> l == ERROR_LISTENER);
    }

    private static void configure(final Settings settings, final Path configsPath, final Path logsPath, boolean useConsole)
        throws IOException {
        Objects.requireNonNull(settings);
        Objects.requireNonNull(configsPath);
        Objects.requireNonNull(logsPath);

        loadLog4jPlugins();

        setLogConfigurationSystemProperty(logsPath, settings);
        // we initialize the status logger immediately otherwise Log4j will complain when we try to get the context
        configureStatusLogger();

        final LoggerContext context = (LoggerContext) LogManager.getContext(false);

        final Set<String> locationsWithDeprecatedPatterns = Collections.synchronizedSet(new HashSet<>());
        final List<AbstractConfiguration> configurations = new ArrayList<>();
        /*
         * Subclass the properties configurator to hack the new pattern in
         * place so users don't have to change log4j2.properties in
         * a minor release. In 7.0 we'll remove this and force users to
         * change log4j2.properties. If they don't customize log4j2.properties
         * then they won't have to do anything anyway.
         *
         * Everything in this subclass that isn't marked as a hack is copied
         * from log4j2's source.
         */
        final PropertiesConfigurationFactory factory = new PropertiesConfigurationFactory() {
            @Override
            public PropertiesConfiguration getConfiguration(final LoggerContext loggerContext, final ConfigurationSource source) {
                final Properties properties = new Properties();
                try (InputStream configStream = source.getInputStream()) {
                    properties.load(configStream);
                } catch (final IOException ioe) {
                    throw new ConfigurationException("Unable to load " + source.toString(), ioe);
                }
                // Hack the new pattern into place
                for (String name : properties.stringPropertyNames()) {
                    if (false == name.endsWith(".pattern")) continue;
                    // Null is weird here but we can't do anything with it so ignore it
                    String value = properties.getProperty(name);
                    if (value == null) continue;
                    // Tests don't need to be changed
                    if (value.contains("%test_thread_info")) continue;
                    /*
                     * Patterns without a marker are sufficiently customized
                     * that we don't have an opinion about them.
                     */
                    if (false == value.contains("%marker")) continue;
                    if (false == value.contains("%node_name")) {
                        locationsWithDeprecatedPatterns.add(source.getLocation());
                        properties.setProperty(name, value.replace("%marker", "[%node_name]%marker "));
                    }
                }

                // end hack
                return new PropertiesConfigurationBuilder().setConfigurationSource(source)
                    .setRootProperties(properties)
                    .setLoggerContext(loggerContext)
                    .build();
            }
        };
        final Set<FileVisitOption> options = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
        Files.walkFileTree(configsPath, options, Integer.MAX_VALUE, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                if (file.getFileName().toString().equals("log4j2.properties")) {
                    configurations.add((PropertiesConfiguration) factory.getConfiguration(context, file.toString(), file.toUri()));
                }
                return FileVisitResult.CONTINUE;
            }
        });
        assert configurations.isEmpty() == false;

        configurations.add(createStaticConfiguration(context));

        context.start(new CompositeConfiguration(configurations));

        configureLoggerLevels(settings);

        final String deprecatedLocationsString = String.join("\n  ", locationsWithDeprecatedPatterns);
        if (deprecatedLocationsString.length() > 0) {
            LogManager.getLogger(LogConfigurator.class)
                .warn(
                    "Some logging configurations have %marker but don't have %node_name. "
                        + "We will automatically add %node_name to the pattern to ease the migration for users who customize "
                        + "log4j2.properties but will stop this behavior in 7.0. You should manually replace `%node_name` with "
                        + "`[%node_name]%marker ` in these locations:\n  {}",
                    deprecatedLocationsString
                );
        }

        JULBridge.install();

        // Redirect stdout/stderr to log4j. While we ensure Elasticsearch code does not write to those streams,
        // third party libraries may do that. Note that we do NOT close the streams because other code may have
        // grabbed a handle to the streams and intend to write to it, eg log4j for writing to the console
        System.setOut(new PrintStream(new LoggingOutputStream(LogManager.getLogger("stdout"), Level.INFO), false, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(new LoggingOutputStream(LogManager.getLogger("stderr"), Level.WARN), false, StandardCharsets.UTF_8));

        final Logger rootLogger = LogManager.getRootLogger();
        Appender appender = Loggers.findAppender(rootLogger, ConsoleAppender.class);
        if (appender != null) {
            if (useConsole) {
                consoleAppender = appender;
            } else {
                Loggers.removeAppender(rootLogger, appender);
            }
        }
    }

    /**
     * Creates a log4j configuration that is not changeable by users.
     */
    private static AbstractConfiguration createStaticConfiguration(LoggerContext context) {
        var builder = new DefaultConfigurationBuilder<>();
        builder.setConfigurationSource(ConfigurationSource.NULL_SOURCE);
        builder.setLoggerContext(context);

        // adding filters for confusing Lucene messages
        addRegexFilter(builder, "org.apache.lucene.store.MemorySegmentIndexInputProvider", "Using MemorySegmentIndexInput.*");
        addRegexFilter(builder, "org.apache.lucene.util.VectorUtilProvider", ".* incubator module is not readable.*");

        return builder.build();
    }

    private static void addRegexFilter(DefaultConfigurationBuilder<BuiltConfiguration> builder, String loggerName, String pattern) {
        var filterBuilder = builder.newFilter("RegexFilter", Filter.Result.DENY, Filter.Result.NEUTRAL);
        filterBuilder.addAttribute("regex", pattern);
        var loggerBuilder = builder.newLogger(loggerName);
        loggerBuilder.add(filterBuilder);
        builder.add(loggerBuilder);
    }

    /**
     * Removes the appender for the console, if one exists.
     */
    public static Appender removeConsoleAppender() {
        Appender appender = consoleAppender;
        if (appender != null) {
            Loggers.removeAppender(LogManager.getRootLogger(), appender);
            consoleAppender = null;
        }
        return appender;
    }

    private static void configureStatusLogger() {
        final ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();
        builder.setStatusLevel(Level.ERROR);
        Configurator.initialize(builder.build());
    }

    /**
     * Configures the logging levels for loggers configured in the specified settings.
     *
     * @param settings the settings from which logger levels will be extracted
     */
    private static void configureLoggerLevels(final Settings settings) {
        if (Loggers.LOG_DEFAULT_LEVEL_SETTING.exists(settings)) {
            final Level level = Loggers.LOG_DEFAULT_LEVEL_SETTING.get(settings);
            Loggers.setLevel(LogManager.getRootLogger(), level);
        }
        Loggers.LOG_LEVEL_SETTING.getAllConcreteSettings(settings)
            // do not set a log level for a logger named level (from the default log setting)
            .filter(s -> s.getKey().equals(Loggers.LOG_DEFAULT_LEVEL_SETTING.getKey()) == false)
            .forEach(s -> {
                final Level level = s.get(settings);
                Loggers.setLevel(LogManager.getLogger(s.getKey().substring("logger.".length())), level);
            });
    }

    /**
     * Set system properties that can be used in configuration files to specify paths and file patterns for log files. We expose three
     * properties here:
     * <ul>
     * <li>
     * {@code es.logs.base_path} the base path containing the log files
     * </li>
     * <li>
     * {@code es.logs.cluster_name} the cluster name, used as the prefix of log filenames in the default configuration
     * </li>
     * <li>
     * {@code es.logs.node_name} the node name, can be used as part of log filenames
     * </li>
     * </ul>
     *
     * @param logsPath the path to the log files
     * @param settings the settings to extract the cluster and node names
     */
    @SuppressForbidden(reason = "sets system property for logging configuration")
    private static void setLogConfigurationSystemProperty(final Path logsPath, final Settings settings) {
        System.setProperty("es.logs.base_path", logsPath.toString());
        System.setProperty("es.logs.cluster_name", ClusterName.CLUSTER_NAME_SETTING.get(settings).value());
        System.setProperty("es.logs.node_name", Node.NODE_NAME_SETTING.get(settings));
    }

}
