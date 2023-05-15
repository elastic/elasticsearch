/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.LockObtainFailedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.admin.indices.rollover.Condition;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.ComponentTemplateMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.DataStreamMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.cli.EnvironmentAwareCommand;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeMetadata;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

public abstract class ElasticsearchNodeCommand extends EnvironmentAwareCommand {
    private static final Logger logger = LogManager.getLogger(ElasticsearchNodeCommand.class);
    protected static final String DELIMITER = "------------------------------------------------------------------------\n";
    static final String STOP_WARNING_MSG = DELIMITER + "\n" + "    WARNING: Elasticsearch MUST be stopped before running this tool." + "\n";
    protected static final String FAILED_TO_OBTAIN_NODE_LOCK_MSG = "failed to lock node's directory, is Elasticsearch still running?";
    protected static final String ABORTED_BY_USER_MSG = "aborted by user";
    static final String NO_NODE_FOLDER_FOUND_MSG = "no node folder is found in data folder(s), node has not been started yet?";
    static final String NO_NODE_METADATA_FOUND_MSG = "no node meta data is found, node has not been started yet?";
    protected static final String CS_MISSING_MSG = "cluster state is empty, cluster has never been bootstrapped?";

    // fake the registry here, as command-line tools are not loading plugins, and ensure that it preserves the parsed XContent
    public static final NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(ClusterModule.getNamedXWriteables()) {

        @SuppressWarnings("unchecked")
        @Override
        public <T, C> T parseNamedObject(Class<T> categoryClass, String name, XContentParser parser, C context) throws IOException {
            // Currently, two unknown top-level objects are present
            if (Metadata.Custom.class.isAssignableFrom(categoryClass)) {
                if (DataStreamMetadata.TYPE.equals(name)
                    || ComposableIndexTemplateMetadata.TYPE.equals(name)
                    || ComponentTemplateMetadata.TYPE.equals(name)) {
                    // DataStreamMetadata is used inside Metadata class for validation purposes and building the indicesLookup,
                    // therefor even es node commands need to be able to parse it.
                    return super.parseNamedObject(categoryClass, name, parser, context);
                    // TODO: Try to parse other named objects (e.g. stored scripts, ingest pipelines) that are part of core es as well?
                    // Note that supporting PersistentTasksCustomMetadata is trickier, because PersistentTaskParams is a named object too.
                } else {
                    return (T) new UnknownMetadataCustom(name, parser.mapOrdered());
                }
            }
            if (Condition.class.isAssignableFrom(categoryClass)) {
                // The parsing for conditions is a bit weird as these represent JSON primitives (strings or numbers)
                // TODO: Make Condition non-pluggable
                assert parser.currentToken() == XContentParser.Token.FIELD_NAME : parser.currentToken();
                if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
                    throw new UnsupportedOperationException("Unexpected token for Condition: " + parser.currentToken());
                }
                parser.nextToken();
                assert parser.currentToken().isValue() : parser.currentToken();
                if (parser.currentToken().isValue() == false) {
                    throw new UnsupportedOperationException("Unexpected token for Condition: " + parser.currentToken());
                }
                return (T) new UnknownCondition(name, parser.objectText());
            }
            assert false : "Unexpected category class " + categoryClass + " for name " + name;
            throw new UnsupportedOperationException("Unexpected category class " + categoryClass + " for name " + name);
        }
    };

    public ElasticsearchNodeCommand(String description) {
        super(description);
    }

    public static PersistedClusterStateService createPersistedClusterStateService(Settings settings, Path[] dataPaths) throws IOException {
        final NodeMetadata nodeMetadata = PersistedClusterStateService.nodeMetadata(dataPaths);
        if (nodeMetadata == null) {
            throw new ElasticsearchException(NO_NODE_METADATA_FOUND_MSG);
        }

        String nodeId = nodeMetadata.nodeId();
        return new PersistedClusterStateService(
            dataPaths,
            nodeId,
            namedXContentRegistry,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            () -> 0L
        );
    }

    public static ClusterState clusterState(Environment environment, PersistedClusterStateService.OnDiskState onDiskState) {
        return ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.get(environment.settings()))
            .version(onDiskState.lastAcceptedVersion)
            .metadata(onDiskState.metadata)
            .build();
    }

    public static Tuple<Long, ClusterState> loadTermAndClusterState(PersistedClusterStateService psf, Environment env) throws IOException {
        final PersistedClusterStateService.OnDiskState bestOnDiskState = psf.loadBestOnDiskState();
        if (bestOnDiskState.empty()) {
            throw new ElasticsearchException(CS_MISSING_MSG);
        }
        return Tuple.tuple(bestOnDiskState.currentTerm, clusterState(env, bestOnDiskState));
    }

    protected void processDataPaths(Terminal terminal, OptionSet options, Environment env) throws IOException, UserException {
        terminal.println(Terminal.Verbosity.VERBOSE, "Obtaining lock for node");
        try (NodeEnvironment.NodeLock lock = new NodeEnvironment.NodeLock(logger, env, Files::exists)) {
            final Path[] dataPaths = Arrays.stream(lock.getDataPaths()).filter(Objects::nonNull).map(p -> p.path).toArray(Path[]::new);
            if (dataPaths.length == 0) {
                throw new ElasticsearchException(NO_NODE_FOLDER_FOUND_MSG);
            }
            processDataPaths(terminal, dataPaths, options, env);
        } catch (LockObtainFailedException e) {
            throw new ElasticsearchException(FAILED_TO_OBTAIN_NODE_LOCK_MSG, e);
        }
    }

    protected static void confirm(Terminal terminal, String msg) {
        terminal.println(msg);
        String text = terminal.readText("Confirm [y/N] ");
        if (text.equalsIgnoreCase("y") == false) {
            throw new ElasticsearchException(ABORTED_BY_USER_MSG);
        }
    }

    @Override
    public final void execute(Terminal terminal, OptionSet options, Environment env, ProcessInfo processInfo) throws Exception {
        terminal.println(STOP_WARNING_MSG);
        if (validateBeforeLock(terminal, env)) {
            processDataPaths(terminal, options, env);
        }
    }

    /**
     * Validate that the command can run before taking any locks.
     * @param terminal the terminal to print to
     * @param env the env to validate.
     * @return true to continue, false to stop (must print message in validate).
     */
    protected boolean validateBeforeLock(Terminal terminal, Environment env) {
        return true;
    }

    /**
     * Process the paths. Locks for the paths is held during this method invocation.
     * @param terminal the terminal to use for messages
     * @param dataPaths the paths of the node to process
     * @param options the command line options
     * @param env the env of the node to process
     */
    protected abstract void processDataPaths(Terminal terminal, Path[] dataPaths, OptionSet options, Environment env) throws IOException,
        UserException;

    protected static NodeEnvironment.DataPath[] toDataPaths(Path[] paths) {
        return Arrays.stream(paths).map(ElasticsearchNodeCommand::createDataPath).toArray(NodeEnvironment.DataPath[]::new);
    }

    private static NodeEnvironment.DataPath createDataPath(Path path) {
        try {
            return new NodeEnvironment.DataPath(path);
        } catch (IOException e) {
            throw new ElasticsearchException("Unable to investigate path [" + path + "]", e);
        }
    }

    // package-private for testing
    OptionParser getParser() {
        return parser;
    }

    public record UnknownMetadataCustom(String name, Map<String, Object> contents) implements Metadata.Custom {

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.API, Metadata.XContentContext.GATEWAY);
        }

        @Override
        public Diff<Metadata.Custom> diff(Metadata.Custom previousState) {
            assert false;
            throw new UnsupportedOperationException();
        }

        @Override
        public String getWriteableName() {
            return name;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            assert false;
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert false;
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
            return Iterators.single(((builder, params) -> builder.mapContents(contents)));
        }
    }

    public static class UnknownCondition extends Condition<Object> {

        public UnknownCondition(String name, Object value) {
            super(name, null);
            this.value = value;
        }

        @Override
        public String getWriteableName() {
            return name;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert false;
            throw new UnsupportedOperationException();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field(name, value);
        }

        @Override
        public Result evaluate(Stats stats) {
            assert false;
            throw new UnsupportedOperationException();
        }

        @Override
        public Type type() {
            assert false;
            throw new UnsupportedOperationException();
        }
    }
}
