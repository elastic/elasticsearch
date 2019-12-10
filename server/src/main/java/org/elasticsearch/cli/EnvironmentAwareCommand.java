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

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.util.KeyValuePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeMetaData;
import org.elasticsearch.gateway.LucenePersistedStateFactory;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.node.InternalSettingsPreparer;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** A cli command which requires an {@link org.elasticsearch.env.Environment} to use current paths and settings. */
public abstract class EnvironmentAwareCommand extends Command {

    protected static final Logger logger = LogManager.getLogger(EnvironmentAwareCommand.class);

    public static final String NO_NODE_METADATA_FOUND_MSG = "no node meta data is found, node has not been started yet?";

    private final OptionSpec<KeyValuePair> settingOption;

    /**
     * Construct the command with the specified command description. This command will have logging configured without reading Elasticsearch
     * configuration files.
     *
     * @param description the command description
     */
    public EnvironmentAwareCommand(final String description) {
        this(description, CommandLoggingConfigurator::configureLoggingWithoutConfig);
    }

    /**
     * Construct the command with the specified command description and runnable to execute before main is invoked. Commands constructed
     * with this constructor must take ownership of configuring logging.
     *
     * @param description the command description
     * @param beforeMain the before-main runnable
     */
    public EnvironmentAwareCommand(final String description, final Runnable beforeMain) {
        super(description, beforeMain);
        this.settingOption = parser.accepts("E", "Configure a setting").withRequiredArg().ofType(KeyValuePair.class);
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options) throws Exception {
        final Map<String, String> settings = new HashMap<>();
        for (final KeyValuePair kvp : settingOption.values(options)) {
            if (kvp.value.isEmpty()) {
                throw new UserException(ExitCodes.USAGE, "setting [" + kvp.key + "] must not be empty");
            }
            if (settings.containsKey(kvp.key)) {
                final String message = String.format(
                        Locale.ROOT,
                        "setting [%s] already set, saw [%s] and [%s]",
                        kvp.key,
                        settings.get(kvp.key),
                        kvp.value);
                throw new UserException(ExitCodes.USAGE, message);
            }
            settings.put(kvp.key, kvp.value);
        }

        putSystemPropertyIfSettingIsMissing(settings, "path.data", "es.path.data");
        putSystemPropertyIfSettingIsMissing(settings, "path.home", "es.path.home");
        putSystemPropertyIfSettingIsMissing(settings, "path.logs", "es.path.logs");

        execute(terminal, options, createEnv(settings));
    }

    /** Create an {@link Environment} for the command to use. Overrideable for tests. */
    protected Environment createEnv(final Map<String, String> settings) throws UserException {
        return createEnv(Settings.EMPTY, settings);
    }

    /** Create an {@link Environment} for the command to use. Overrideable for tests. */
    protected final Environment createEnv(final Settings baseSettings, final Map<String, String> settings) throws UserException {
        final String esPathConf = System.getProperty("es.path.conf");
        if (esPathConf == null) {
            throw new UserException(ExitCodes.CONFIG, "the system property [es.path.conf] must be set");
        }
        return InternalSettingsPreparer.prepareEnvironment(baseSettings, settings,
            getConfigPath(esPathConf),
            // HOSTNAME is set by elasticsearch-env and elasticsearch-env.bat so it is always available
            () -> System.getenv("HOSTNAME"));
    }

    @SuppressForbidden(reason = "need path to construct environment")
    private static Path getConfigPath(final String pathConf) {
        return Paths.get(pathConf);
    }

    /** Ensure the given setting exists, reading it from system properties if not already set. */
    private static void putSystemPropertyIfSettingIsMissing(final Map<String, String> settings, final String setting, final String key) {
        final String value = System.getProperty(key);
        if (value != null) {
            if (settings.containsKey(setting)) {
                final String message =
                        String.format(
                                Locale.ROOT,
                                "duplicate setting [%s] found via command-line [%s] and system property [%s]",
                                setting,
                                settings.get(setting),
                                value);
                throw new IllegalArgumentException(message);
            } else {
                settings.put(setting, value);
            }
        }
    }

    public static String loadNodeId(Terminal terminal, Path[] dataPaths) throws IOException {
        terminal.println(Terminal.Verbosity.VERBOSE, "Loading node metadata");
        final NodeMetaData nodeMetaData = NodeMetaData.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY, dataPaths);
        if (nodeMetaData == null) {
            throw new ElasticsearchException(NO_NODE_METADATA_FOUND_MSG);
        }

        String nodeId = nodeMetaData.nodeId();
        terminal.println(Terminal.Verbosity.VERBOSE, "Current nodeId is " + nodeId);
        return nodeId;
    }

    public static LucenePersistedStateFactory createLucenePersistedStateFactory(Path[] dataPaths, String nodeId) {
        final NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(
            Stream.of(ClusterModule.getNamedXWriteables().stream(), IndicesModule.getNamedXContents().stream())
                .flatMap(Function.identity())
                .collect(Collectors.toList()));
        return new LucenePersistedStateFactory(dataPaths, nodeId, true, false, namedXContentRegistry, BigArrays.NON_RECYCLING_INSTANCE,
            // do not load legacy files
            new LucenePersistedStateFactory.LegacyLoader() {
                @Override
                public Tuple<Manifest, MetaData> loadClusterState() {
                    return null;
                }

                @Override
                public void clean() {

                }
            });
    }

    public static BiFunction<Long, MetaData, ClusterState> clusterState(Environment environment) {
        return (version, metadata) -> ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.get(environment.settings()))
            .version(version)
            .metaData(metadata)
            .build();
    }

    /** Execute the command with the initialized {@link Environment}. */
    protected abstract void execute(Terminal terminal, OptionSet options, Environment env) throws Exception;

}
