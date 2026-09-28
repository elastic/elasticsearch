/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.internal;

import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.ConfigurationException;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Order;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.properties.PropertiesConfiguration;
import org.apache.logging.log4j.core.config.properties.PropertiesConfigurationBuilder;
import org.apache.logging.log4j.core.config.properties.PropertiesConfigurationFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Log4j {@code ConfigurationFactory} that strips {@code %node_name} / {@code %cluster_name}
 * (and their {@code ES}-prefixed aliases) out of {@code .properties} log4j configs at load
 * time, before log4j hands them to its pattern parser.
 *
 * <p>Auto-discovered by log4j via the plugin cache: the {@link Plugin @Plugin} annotation
 * causes log4j-core's {@code PluginProcessor} (an annotation processor) to add this class
 * to the {@code META-INF/.../Log4j2Plugins.dat} entry inside the shipped jar, which log4j
 * merges with every other classpath jar's plugin cache at {@code LoggerContext} startup.
 * With {@link Order @Order(10)} we outrank the built-in {@code PropertiesConfigurationFactory}
 * ({@code @Order(8)}) so we get first pick for {@code .properties} sources — verified via
 * {@code OrderComparator}: "larger value means higher priority".
 *
 * <p>Why strip: benchmarked code that touches log4j at class-init time (Lucene codecs,
 * ES internal helpers reached via SPI, …) triggers a {@code LoggerContext} that reads the
 * first {@code log4j2*.properties} it finds on the classpath. Historically those patterns
 * contain {@code [%node_name]}, whose converter ({@code NodeNamePatternConverter} in
 * {@code :server}) requires a {@code SetOnce} to have been populated before pattern parsing
 * — a fragile ordering constraint the benchmark suite has repeatedly stumbled over.
 * Stripping the tokens sidesteps the ordering entirely and removes the runtime dependency
 * on the {@code :server} pattern converters for any classpath they end up on.
 *
 * <p>Only {@code .properties} configs are intercepted. If a future dep introduces an XML,
 * YAML, or JSON log4j config on the benchmarks classpath, add a sibling factory for that
 * source type — the built-ins live in {@code org.apache.logging.log4j.core.config.xml} etc.
 */
@Plugin(name = "BenchmarkConfigurationFactory", category = ConfigurationFactory.CATEGORY)
@Order(10)
public final class BenchmarkConfigurationFactory extends PropertiesConfigurationFactory {

    // Matches %node_name, %ESnode_name, %cluster_name, %EScluster_name — optionally wrapped
    // in [] with surrounding whitespace. Covers every @ConverterKeys value of
    // NodeNamePatternConverter ({"ESnode_name", "node_name"}) and
    // ClusterNamePatternConverter ({"EScluster_name", "cluster_name"}) in :server.
    private static final Pattern STRIP = Pattern.compile("\\s*\\[?\\s*%(?:ES)?(?:node_name|cluster_name)\\s*\\]?\\s*");

    @Override
    public PropertiesConfiguration getConfiguration(LoggerContext loggerContext, ConfigurationSource source) {
        Properties props = new Properties();
        try (InputStream in = source.getInputStream()) {
            props.load(in);
        } catch (IOException e) {
            throw new ConfigurationException("Unable to load " + source, e);
        }
        for (String name : props.stringPropertyNames()) {
            if (name.endsWith(".pattern") == false) {
                continue;
            }
            String value = props.getProperty(name);
            if (value == null) {
                continue;
            }
            String stripped = STRIP.matcher(value).replaceAll(" ").trim();
            if (stripped.equals(value) == false) {
                props.setProperty(name, stripped);
            }
        }
        return new PropertiesConfigurationBuilder().setConfigurationSource(source)
            .setRootProperties(props)
            .setLoggerContext(loggerContext)
            .build();
    }
}
