/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.resolve;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.ResolvedExpression;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.crossproject.CrossProjectIndexResolutionValidator;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.action.search.TransportSearchHelper.checkCCSVersionCompatibility;
import static org.elasticsearch.search.crossproject.CrossProjectIndexResolutionValidator.indicesOptionsForCrossProjectFanout;

public class ResolveIndexAction extends ActionType<ResolveIndexAction.Response> {

    public static final ResolveIndexAction INSTANCE = new ResolveIndexAction();
    public static final String NAME = "indices:admin/resolve/index";
    public static final RemoteClusterActionType<Response> REMOTE_TYPE = new RemoteClusterActionType<>(NAME, Response::new);

    private static final TransportVersion RESOLVE_INDEX_MODE_ADDED = TransportVersion.fromName("resolve_index_mode_added");
    private static final TransportVersion RESOLVE_INDEX_MODE_FILTER = TransportVersion.fromName("resolve_index_mode_filter");

    private ResolveIndexAction() {
        super(NAME);
    }

    public static class Request extends LegacyActionRequest implements IndicesRequest.Replaceable {

        public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.strictExpandOpen();

        private String[] names;
        private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;
        private EnumSet<IndexMode> indexModes = EnumSet.noneOf(IndexMode.class);
        private ResolvedIndexExpressions resolvedIndexExpressions = null;
        private String projectRouting;

        public Request(String[] names) {
            this.names = names;
        }

        public Request(String[] names, IndicesOptions indicesOptions) {
            this.names = names;
            this.indicesOptions = indicesOptions;
        }

        public Request(String[] names, IndicesOptions indicesOptions, @Nullable EnumSet<IndexMode> indexModes) {
            this(names, indicesOptions, indexModes, null);
        }

        public Request(
            String[] names,
            IndicesOptions indicesOptions,
            @Nullable EnumSet<IndexMode> indexModes,
            @Nullable String projectRouting
        ) {
            this.names = names;
            this.indicesOptions = indicesOptions;
            if (indexModes != null) {
                this.indexModes = indexModes;
            }
            this.projectRouting = projectRouting;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.names = in.readStringArray();
            this.indicesOptions = IndicesOptions.readIndicesOptions(in);
            if (in.getTransportVersion().supports(RESOLVE_INDEX_MODE_FILTER)) {
                this.indexModes = in.readEnumSet(IndexMode.class);
            } else {
                this.indexModes = EnumSet.noneOf(IndexMode.class);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(names);
            indicesOptions.writeIndicesOptions(out);
            if (out.getTransportVersion().supports(RESOLVE_INDEX_MODE_FILTER)) {
                out.writeEnumSet(indexModes);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Arrays.equals(names, request.names) && indexModes.equals(request.indexModes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(names), indexModes);
        }

        @Override
        public String[] indices() {
            return names;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            this.names = indices;
            return this;
        }

        @Override
        public boolean allowsRemoteIndices() {
            return true;
        }

        @Override
        public boolean allowsCrossProject() {
            return true;
        }

        @Override
        public void setResolvedIndexExpressions(ResolvedIndexExpressions expressions) {
            this.resolvedIndexExpressions = expressions;
        }

        @Override
        public ResolvedIndexExpressions getResolvedIndexExpressions() {
            return resolvedIndexExpressions;
        }

        @Override
        public boolean includeDataStreams() {
            // request must allow data streams because the index name expression resolver for the action handler assumes it
            return true;
        }

        @Override
        public String getProjectRouting() {
            return projectRouting;
        }
    }

    public static class ResolvedIndexAbstraction {

        static final ParseField NAME_FIELD = new ParseField("name");

        private String name;

        ResolvedIndexAbstraction() {}

        ResolvedIndexAbstraction(String name) {
            this.name = name;
        }

        protected void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public static class ResolvedIndex extends ResolvedIndexAbstraction implements Writeable, ToXContentObject {

        static final ParseField ALIASES_FIELD = new ParseField("aliases");
        static final ParseField ATTRIBUTES_FIELD = new ParseField("attributes");
        static final ParseField DATA_STREAM_FIELD = new ParseField("data_stream");
        static final ParseField MODE_FIELD = new ParseField("mode");

        private final String[] aliases;
        private final String[] attributes;
        private final String dataStream;
        private final IndexMode mode;

        ResolvedIndex(StreamInput in) throws IOException {
            setName(in.readString());
            this.aliases = in.readStringArray();
            this.attributes = in.readStringArray();
            this.dataStream = in.readOptionalString();
            if (in.getTransportVersion().supports(RESOLVE_INDEX_MODE_ADDED)) {
                this.mode = IndexMode.readFrom(in);
            } else {
                this.mode = null;
            }
        }

        ResolvedIndex(String name, String[] aliases, String[] attributes, @Nullable String dataStream, IndexMode mode) {
            super(name);
            this.aliases = aliases;
            this.attributes = attributes;
            this.dataStream = dataStream;
            this.mode = mode;
        }

        public ResolvedIndex copy(String newName) {
            return new ResolvedIndex(newName, aliases, attributes, dataStream, mode);
        }

        public String[] getAliases() {
            return aliases;
        }

        public String[] getAttributes() {
            return attributes;
        }

        public String getDataStream() {
            return dataStream;
        }

        public IndexMode getMode() {
            return mode;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(getName());
            out.writeStringArray(aliases);
            out.writeStringArray(attributes);
            out.writeOptionalString(dataStream);
            if (out.getTransportVersion().supports(RESOLVE_INDEX_MODE_ADDED)) {
                IndexMode.writeTo(mode, out);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(NAME_FIELD.getPreferredName(), getName());
            if (aliases.length > 0) {
                builder.array(ALIASES_FIELD.getPreferredName(), aliases);
            }
            builder.array(ATTRIBUTES_FIELD.getPreferredName(), attributes);
            if (Strings.isNullOrEmpty(dataStream) == false) {
                builder.field(DATA_STREAM_FIELD.getPreferredName(), dataStream);
            }
            if (mode != null) {
                builder.field(MODE_FIELD.getPreferredName(), mode.toString());
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ResolvedIndex index = (ResolvedIndex) o;
            return getName().equals(index.getName())
                && Objects.equals(dataStream, index.dataStream)
                && Arrays.equals(aliases, index.aliases)
                && Arrays.equals(attributes, index.attributes)
                && Objects.equals(mode, index.mode);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(getName(), dataStream);
            result = 31 * result + Objects.hashCode(mode);
            result = 31 * result + Arrays.hashCode(aliases);
            result = 31 * result + Arrays.hashCode(attributes);
            return result;
        }

        @Override
        public String toString() {
            return String.format(
                Locale.ROOT,
                "ResolvedIndex{name=%s, aliases=%s, attributes=%s, dataStream=%s, mode=%s}",
                getName(),
                Arrays.toString(aliases),
                Arrays.toString(attributes),
                dataStream,
                mode
            );
        }
    }

    public static class ResolvedAlias extends ResolvedIndexAbstraction implements Writeable, ToXContentObject {

        static final ParseField INDICES_FIELD = new ParseField("indices");

        private final String[] indices;

        ResolvedAlias(StreamInput in) throws IOException {
            setName(in.readString());
            this.indices = in.readStringArray();
        }

        ResolvedAlias(String name, String[] indices) {
            super(name);
            this.indices = indices;
        }

        public ResolvedAlias copy(String newName) {
            return new ResolvedAlias(newName, indices);
        }

        public String[] getIndices() {
            return indices;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(getName());
            out.writeStringArray(indices);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(NAME_FIELD.getPreferredName(), getName());
            if (indices.length > 0) {
                builder.array(INDICES_FIELD.getPreferredName(), indices);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ResolvedAlias alias = (ResolvedAlias) o;
            return getName().equals(alias.getName()) && Arrays.equals(indices, alias.indices);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(getName());
            result = 31 * result + Arrays.hashCode(indices);
            return result;
        }

        @Override
        public String toString() {
            return String.format(Locale.ROOT, "ResolvedAlias{name=%s, indices=%s}", getName(), Arrays.toString(indices));
        }
    }

    public static class ResolvedDataStream extends ResolvedIndexAbstraction implements Writeable, ToXContentObject {

        static final ParseField BACKING_INDICES_FIELD = new ParseField("backing_indices");
        static final ParseField TIMESTAMP_FIELD = new ParseField("timestamp_field");

        private final String[] backingIndices;
        private final String timestampField;

        ResolvedDataStream(StreamInput in) throws IOException {
            setName(in.readString());
            this.backingIndices = in.readStringArray();
            this.timestampField = in.readString();
        }

        ResolvedDataStream(String name, String[] backingIndices, String timestampField) {
            super(name);
            this.backingIndices = backingIndices;
            this.timestampField = timestampField;
        }

        public ResolvedDataStream copy(String newName) {
            return new ResolvedDataStream(newName, backingIndices, timestampField);
        }

        public String[] getBackingIndices() {
            return backingIndices;
        }

        public String getTimestampField() {
            return timestampField;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(getName());
            out.writeStringArray(backingIndices);
            out.writeString(timestampField);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(NAME_FIELD.getPreferredName(), getName());
            builder.array(BACKING_INDICES_FIELD.getPreferredName(), backingIndices);
            builder.field(TIMESTAMP_FIELD.getPreferredName(), timestampField);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ResolvedDataStream dataStream = (ResolvedDataStream) o;
            return getName().equals(dataStream.getName())
                && timestampField.equals(dataStream.timestampField)
                && Arrays.equals(backingIndices, dataStream.backingIndices);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(getName(), timestampField);
            result = 31 * result + Arrays.hashCode(backingIndices);
            return result;
        }

        @Override
        public String toString() {
            return String.format(
                Locale.ROOT,
                "ResolvedDataStream{name=%s, backingIndices=%s, timestampField=%s}",
                getName(),
                Arrays.toString(backingIndices),
                timestampField
            );
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        static final ParseField INDICES_FIELD = new ParseField("indices");
        static final ParseField ALIASES_FIELD = new ParseField("aliases");
        static final ParseField DATA_STREAMS_FIELD = new ParseField("data_streams");

        private final List<ResolvedIndex> indices;
        private final List<ResolvedAlias> aliases;
        private final List<ResolvedDataStream> dataStreams;
        @Nullable
        private final ResolvedIndexExpressions resolvedIndexExpressions;

        public Response(List<ResolvedIndex> indices, List<ResolvedAlias> aliases, List<ResolvedDataStream> dataStreams) {
            this(indices, aliases, dataStreams, null);
        }

        public Response(
            List<ResolvedIndex> indices,
            List<ResolvedAlias> aliases,
            List<ResolvedDataStream> dataStreams,
            ResolvedIndexExpressions resolvedIndexExpressions
        ) {
            this.indices = indices;
            this.aliases = aliases;
            this.dataStreams = dataStreams;
            this.resolvedIndexExpressions = resolvedIndexExpressions;
        }

        public Response(StreamInput in) throws IOException {
            this.indices = in.readCollectionAsList(ResolvedIndex::new);
            this.aliases = in.readCollectionAsList(ResolvedAlias::new);
            this.dataStreams = in.readCollectionAsList(ResolvedDataStream::new);
            if (in.getTransportVersion().supports(ResolvedIndexExpressions.RESOLVED_INDEX_EXPRESSIONS)) {
                this.resolvedIndexExpressions = in.readOptionalWriteable(ResolvedIndexExpressions::new);
            } else {
                this.resolvedIndexExpressions = null;
            }
        }

        public List<ResolvedIndex> getIndices() {
            return indices;
        }

        public List<ResolvedAlias> getAliases() {
            return aliases;
        }

        public List<ResolvedDataStream> getDataStreams() {
            return dataStreams;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(indices);
            out.writeCollection(aliases);
            out.writeCollection(dataStreams);
            if (out.getTransportVersion().supports(ResolvedIndexExpressions.RESOLVED_INDEX_EXPRESSIONS)) {
                out.writeOptionalWriteable(resolvedIndexExpressions);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.xContentList(INDICES_FIELD.getPreferredName(), indices);
            builder.xContentList(ALIASES_FIELD.getPreferredName(), aliases);
            builder.xContentList(DATA_STREAMS_FIELD.getPreferredName(), dataStreams);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return indices.equals(response.indices) && aliases.equals(response.aliases) && dataStreams.equals(response.dataStreams);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indices, aliases, dataStreams);
        }

        @Nullable
        public ResolvedIndexExpressions getResolvedIndexExpressions() {
            return resolvedIndexExpressions;
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {
        private static final Logger logger = LogManager.getLogger(TransportAction.class);

        private final ClusterService clusterService;
        private final RemoteClusterService remoteClusterService;
        private final ProjectResolver projectResolver;
        private final IndexNameExpressionResolver indexNameExpressionResolver;
        private final boolean ccsCheckCompatibility;
        private final CrossProjectModeDecider crossProjectModeDecider;

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ActionFilters actionFilters,
            ProjectResolver projectResolver,
            Settings settings,
            IndexNameExpressionResolver indexNameExpressionResolver
        ) {
            super(NAME, transportService, actionFilters, Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
            this.clusterService = clusterService;
            this.remoteClusterService = transportService.getRemoteClusterService();
            this.projectResolver = projectResolver;
            this.indexNameExpressionResolver = indexNameExpressionResolver;
            this.crossProjectModeDecider = new CrossProjectModeDecider(settings);
            this.ccsCheckCompatibility = SearchService.CCS_VERSION_CHECK_SETTING.get(clusterService.getSettings());
        }

        @Override
        protected void doExecute(Task task, Request request, final ActionListener<Response> listener) {
            if (ccsCheckCompatibility) {
                checkCCSVersionCompatibility(request);
            }
            final ProjectState projectState = projectResolver.getProjectState(clusterService.state());
            final IndicesOptions originalIndicesOptions = request.indicesOptions();
            final boolean resolveCrossProject = crossProjectModeDecider.resolvesCrossProject(request);
            final Map<String, OriginalIndices> remoteClusterIndices = remoteClusterService.groupIndices(
                resolveCrossProject ? indicesOptionsForCrossProjectFanout(originalIndicesOptions) : originalIndicesOptions,
                request.indices()
            );
            final OriginalIndices localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            List<ResolvedIndex> indices = new ArrayList<>();
            List<ResolvedAlias> aliases = new ArrayList<>();
            List<ResolvedDataStream> dataStreams = new ArrayList<>();
            resolveIndices(localIndices, projectState, indexNameExpressionResolver, indices, aliases, dataStreams, request.indexModes);

            final ResolvedIndexExpressions localResolvedIndexExpressions = request.getResolvedIndexExpressions();
            if (remoteClusterIndices.size() > 0) {
                final int remoteRequests = remoteClusterIndices.size();
                final CountDown completionCounter = new CountDown(remoteRequests);
                final SortedMap<String, Response> remoteResponses = Collections.synchronizedSortedMap(new TreeMap<>());
                final Runnable terminalHandler = () -> {
                    if (completionCounter.countDown()) {
                        if (resolveCrossProject) {
                            // TODO temporary fix: we need to properly handle the case where a remote does not return a result due to
                            // a failure -- in the current version of resolve indices though, these are just silently ignored
                            if (remoteRequests != remoteResponses.size()) {
                                listener.onFailure(
                                    new IllegalStateException(
                                        "expected [" + remoteRequests + "] remote responses but got only [" + remoteResponses.size() + "]"
                                    )
                                );
                                return;
                            }
                            final Exception ex = CrossProjectIndexResolutionValidator.validate(
                                originalIndicesOptions,
                                request.getProjectRouting(),
                                localResolvedIndexExpressions,
                                getResolvedExpressionsByRemote(remoteResponses)
                            );
                            if (ex != null) {
                                listener.onFailure(ex);
                                return;
                            }
                        }
                        mergeResults(remoteResponses, indices, aliases, dataStreams, request.indexModes);
                        listener.onResponse(new Response(indices, aliases, dataStreams));
                    }
                };

                // make the cross-cluster calls
                for (Map.Entry<String, OriginalIndices> remoteIndices : remoteClusterIndices.entrySet()) {
                    String clusterAlias = remoteIndices.getKey();
                    OriginalIndices originalIndices = remoteIndices.getValue();
                    var remoteClusterClient = remoteClusterService.getRemoteClusterClient(
                        clusterAlias,
                        EsExecutors.DIRECT_EXECUTOR_SERVICE,
                        RemoteClusterService.DisconnectedStrategy.RECONNECT_UNLESS_SKIP_UNAVAILABLE
                    );
                    Request remoteRequest = new Request(originalIndices.indices(), originalIndices.indicesOptions());
                    remoteClusterClient.execute(ResolveIndexAction.REMOTE_TYPE, remoteRequest, ActionListener.wrap(response -> {
                        remoteResponses.put(clusterAlias, response);
                        terminalHandler.run();
                    }, failure -> {
                        logger.info("failed to resolve indices on remote cluster [" + clusterAlias + "]", failure);
                        terminalHandler.run();
                    }));
                }
            } else {
                if (resolveCrossProject) {
                    // we still need to call response validation for local results, since qualified expressions like `_origin:index` or
                    // `<alias-pattern-matching-origin-only>:index` also get deferred validation
                    final Exception ex = CrossProjectIndexResolutionValidator.validate(
                        originalIndicesOptions,
                        request.getProjectRouting(),
                        localResolvedIndexExpressions,
                        Map.of()
                    );
                    if (ex != null) {
                        listener.onFailure(ex);
                        return;
                    }
                }
                listener.onResponse(new Response(indices, aliases, dataStreams, localResolvedIndexExpressions));
            }
        }

        private Map<String, ResolvedIndexExpressions> getResolvedExpressionsByRemote(Map<String, Response> remoteResponses) {
            return remoteResponses.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> {
                final ResolvedIndexExpressions resolvedIndexExpressions = e.getValue().getResolvedIndexExpressions();
                assert resolvedIndexExpressions != null
                    : "remote response from cluster [" + e.getKey() + "] is missing resolved index expressions";
                return resolvedIndexExpressions;
            }));
        }

        /**
         * Resolves the specified names and/or wildcard expressions to index abstractions. Returns results in the supplied lists.
         *
         * @param localIndices The names and wildcard expressions to resolve
         * @param projectState Project state
         * @param resolver     Resolver instance for matching names
         * @param indices      List containing any matching indices
         * @param aliases      List containing any matching aliases
         * @param dataStreams  List containing any matching data streams
         */
        static void resolveIndices(
            @Nullable OriginalIndices localIndices,
            ProjectState projectState,
            IndexNameExpressionResolver resolver,
            List<ResolvedIndex> indices,
            List<ResolvedAlias> aliases,
            List<ResolvedDataStream> dataStreams,
            Set<IndexMode> indexModes
        ) {
            if (localIndices == null) {
                return;
            }
            resolveIndices(
                localIndices.indices(),
                localIndices.indicesOptions(),
                projectState,
                resolver,
                indices,
                aliases,
                dataStreams,
                indexModes
            );
        }

        // Shortcut for tests that don't need index mode filtering
        static void resolveIndices(
            String[] names,
            IndicesOptions indicesOptions,
            ProjectState projectState,
            IndexNameExpressionResolver resolver,
            List<ResolvedIndex> indices,
            List<ResolvedAlias> aliases,
            List<ResolvedDataStream> dataStreams
        ) {
            resolveIndices(names, indicesOptions, projectState, resolver, indices, aliases, dataStreams, Collections.emptySet());
        }

        /**
         * Resolves the specified names and/or wildcard expressions to index abstractions. Returns results in the supplied lists.
         *
         * @param names          The names and wildcard expressions to resolve
         * @param indicesOptions Options for expanding wildcards to indices with different states
         * @param projectState   Cluster state
         * @param resolver       Resolver instance for matching names
         * @param indices        List containing any matching indices
         * @param aliases        List containing any matching aliases
         * @param dataStreams    List containing any matching data streams
         */
        static void resolveIndices(
            String[] names,
            IndicesOptions indicesOptions,
            ProjectState projectState,
            IndexNameExpressionResolver resolver,
            List<ResolvedIndex> indices,
            List<ResolvedAlias> aliases,
            List<ResolvedDataStream> dataStreams,
            Set<IndexMode> indexModes
        ) {
            // redundant check to ensure that we don't resolve the list of empty names to "all" in this context
            if (names.length == 0) {
                return;
            }
            // TODO This is a dirty hack around the IndexNameExpressionResolver optimisation for "*" as described in:
            // https://github.com/elastic/elasticsearch/issues/92903.
            // A standalone "*" expression is resolved slightly differently from a "*" embedded in another expression, eg "idx,*".
            // The difference is only slight, and it usually doesn't cause problems (see
            // https://github.com/elastic/elasticsearch/issues/92911 for a description of a problem).
            // But in the case of the Resolve index API, the difference is observable, because resolving standalone "*" cannot show
            // aliases (only indices and datastreams). The Resolve index API needs to show the aliases that match wildcards.
            if (names.length == 1 && (Metadata.ALL.equals(names[0]) || Regex.isMatchAllPattern(names[0]))) {
                names = new String[] { "**" };
            }
            Set<ResolvedExpression> resolvedIndexAbstractions = resolver.resolveExpressions(
                projectState.metadata(),
                indicesOptions,
                true,
                names
            );
            for (ResolvedExpression s : resolvedIndexAbstractions) {
                enrichIndexAbstraction(projectState, s, indices, aliases, dataStreams, indexModes);
            }
            indices.sort(Comparator.comparing(ResolvedIndexAbstraction::getName));
            aliases.sort(Comparator.comparing(ResolvedIndexAbstraction::getName));
            dataStreams.sort(Comparator.comparing(ResolvedIndexAbstraction::getName));
        }

        /**
         * Merge the results from remote clusters into the local results lists.
         * This will also do index mode filtering (if requested), as the remote cluster might be too old to do it itself.
         */
        private static void mergeResults(
            Map<String, Response> remoteResponses,
            List<ResolvedIndex> indices,
            List<ResolvedAlias> aliases,
            List<ResolvedDataStream> dataStreams,
            Set<IndexMode> indexModes
        ) {
            for (Map.Entry<String, Response> responseEntry : remoteResponses.entrySet()) {
                String clusterAlias = responseEntry.getKey();
                Response response = responseEntry.getValue();
                for (ResolvedIndex index : response.indices) {
                    // We want to filter by mode here because the linked cluster might be too old to be able to filter
                    if (indexModes.isEmpty() == false && indexModes.contains(index.getMode()) == false) {
                        continue;
                    }
                    indices.add(index.copy(RemoteClusterAware.buildRemoteIndexName(clusterAlias, index.getName())));
                }
                Set<String> indexNames = indices.stream().map(ResolvedIndexAbstraction::getName).collect(Collectors.toSet());
                for (ResolvedAlias alias : response.aliases) {
                    if (indexModes.isEmpty() == false) {
                        // We filter out indices that are not included in the main index list after index mode filtering
                        String[] filteredIndices = Arrays.stream(alias.getIndices())
                            .filter(idxName -> indexNames.contains(RemoteClusterAware.buildRemoteIndexName(clusterAlias, idxName)))
                            .toArray(String[]::new);
                        if (filteredIndices.length == 0) {
                            // If this alias points to no indices after filtering, we skip it
                            continue;
                        }
                        alias = new ResolvedAlias(RemoteClusterAware.buildRemoteIndexName(clusterAlias, alias.getName()), filteredIndices);
                    } else {
                        alias = alias.copy(RemoteClusterAware.buildRemoteIndexName(clusterAlias, alias.getName()));
                    }
                    aliases.add(alias);
                }
                for (ResolvedDataStream dataStream : response.dataStreams) {
                    if (indexModes.isEmpty() == false) {
                        // We filter out indices that are not included in the main index list after index mode filtering
                        String[] filteredBackingIndices = Arrays.stream(dataStream.getBackingIndices())
                            .filter(idxName -> indexNames.contains(RemoteClusterAware.buildRemoteIndexName(clusterAlias, idxName)))
                            .toArray(String[]::new);
                        if (filteredBackingIndices.length == 0) {
                            // If this data stream points to no backing indices after filtering, we skip it
                            continue;
                        }
                        dataStream = new ResolvedDataStream(
                            RemoteClusterAware.buildRemoteIndexName(clusterAlias, dataStream.getName()),
                            filteredBackingIndices,
                            dataStream.getTimestampField()
                        );
                    } else {
                        dataStream = dataStream.copy(RemoteClusterAware.buildRemoteIndexName(clusterAlias, dataStream.getName()));
                    }
                    dataStreams.add(dataStream);
                }
            }
        }

        private static Predicate<Index> indexModeFilter(ProjectState projectState, Set<IndexMode> indexModes) {
            if (indexModes.isEmpty()) {
                return index -> true;
            }
            return index -> {
                IndexMetadata indexMetadata = projectState.metadata().index(index);
                IndexMode mode = indexMetadata.getIndexMode() == null ? IndexMode.STANDARD : indexMetadata.getIndexMode();
                return indexModes.contains(mode);
            };
        }

        private static void enrichIndexAbstraction(
            ProjectState projectState,
            ResolvedExpression resolvedExpression,
            List<ResolvedIndex> indices,
            List<ResolvedAlias> aliases,
            List<ResolvedDataStream> dataStreams,
            Set<IndexMode> indexModes
        ) {
            SortedMap<String, IndexAbstraction> indicesLookup = projectState.metadata().getIndicesLookup();
            IndexAbstraction ia = indicesLookup.get(resolvedExpression.resource());
            var filterPredicate = indexModeFilter(projectState, indexModes);
            if (ia != null) {
                switch (ia.getType()) {
                    case CONCRETE_INDEX -> {
                        if (filterPredicate.test(ia.getWriteIndex()) == false) {
                            return;
                        }
                        IndexMetadata writeIndex = projectState.metadata().index(ia.getWriteIndex());
                        IndexMode mode = writeIndex.getIndexMode() == null ? IndexMode.STANDARD : writeIndex.getIndexMode();
                        String[] aliasNames = writeIndex.getAliases().keySet().stream().sorted().toArray(String[]::new);
                        List<Attribute> attributes = new ArrayList<>();
                        attributes.add(writeIndex.getState() == IndexMetadata.State.OPEN ? Attribute.OPEN : Attribute.CLOSED);
                        if (ia.isHidden()) {
                            attributes.add(Attribute.HIDDEN);
                        }
                        if (ia.isSystem()) {
                            attributes.add(Attribute.SYSTEM);
                        }
                        final boolean isFrozen = writeIndex.getSettings().getAsBoolean("index.frozen", false);
                        if (isFrozen) {
                            attributes.add(Attribute.FROZEN);
                        }
                        attributes.sort(Comparator.comparing(e -> e.name().toLowerCase(Locale.ROOT)));
                        indices.add(
                            new ResolvedIndex(
                                ia.getName(),
                                aliasNames,
                                attributes.stream().map(Enum::name).map(e -> e.toLowerCase(Locale.ROOT)).toArray(String[]::new),
                                ia.getParentDataStream() == null ? null : ia.getParentDataStream().getName(),
                                mode
                            )
                        );
                    }
                    case ALIAS -> {
                        String[] indexNames = getAliasIndexStream(resolvedExpression, ia, projectState.metadata()).filter(filterPredicate)
                            .map(Index::getName)
                            .toArray(String[]::new);
                        if (indexModes.isEmpty() == false && indexNames.length == 0) {
                            return;
                        }
                        Arrays.sort(indexNames);
                        aliases.add(new ResolvedAlias(ia.getName(), indexNames));
                    }
                    case DATA_STREAM -> {
                        DataStream dataStream = (DataStream) ia;
                        Stream<Index> dataStreamIndices = resolvedExpression.selector() == null
                            ? dataStream.getIndices().stream()
                            : switch (resolvedExpression.selector()) {
                                case DATA -> dataStream.getDataComponent().getIndices().stream();
                                case FAILURES -> dataStream.getFailureIndices().stream();
                            };
                        String[] backingIndices = dataStreamIndices.filter(filterPredicate).map(Index::getName).toArray(String[]::new);
                        if (indexModes.isEmpty() == false && backingIndices.length == 0) {
                            return;
                        }
                        dataStreams.add(new ResolvedDataStream(dataStream.getName(), backingIndices, DataStream.TIMESTAMP_FIELD_NAME));
                    }
                    default -> throw new IllegalStateException("unknown index abstraction type: " + ia.getType());
                }
            }
        }

        private static Stream<Index> getAliasIndexStream(
            ResolvedExpression resolvedExpression,
            IndexAbstraction ia,
            ProjectMetadata metadata
        ) {
            Stream<Index> aliasIndices;
            if (resolvedExpression.selector() == null) {
                aliasIndices = ia.getIndices().stream();
            } else {
                aliasIndices = switch (resolvedExpression.selector()) {
                    case DATA -> ia.getIndices().stream();
                    case FAILURES -> {
                        assert ia.isDataStreamRelated() : "Illegal selector [failures] used on non data stream alias";
                        yield ia.getFailureIndices(metadata).stream();
                    }
                };
            }
            return aliasIndices;
        }

        enum Attribute {
            OPEN,
            CLOSED,
            HIDDEN,
            SYSTEM,
            FROZEN
        }
    }
}
