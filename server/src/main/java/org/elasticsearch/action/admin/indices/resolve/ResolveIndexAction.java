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
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.RemoteClusterActionType;
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
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.SearchService;
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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.elasticsearch.action.search.TransportSearchHelper.checkCCSVersionCompatibility;

public class ResolveIndexAction extends ActionType<ResolveIndexAction.Response> {

    public static final ResolveIndexAction INSTANCE = new ResolveIndexAction();
    public static final String NAME = "indices:admin/resolve/index";
    public static final RemoteClusterActionType<Response> REMOTE_TYPE = new RemoteClusterActionType<>(NAME, Response::new);

    private ResolveIndexAction() {
        super(NAME);
    }

    public static class Request extends LegacyActionRequest implements IndicesRequest.CrossProjectResolvable, IndicesRequest.Resolvable {

        public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.strictExpandOpen();

        private String[] names;
        private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;
        private List<RewrittenExpression> rewrittenExpressions;
        private boolean includeResolvedInResponse = false;
        @Nullable
        private Map<String, List<String>> resolved = null;

        public Request(String[] names) {
            this.names = names;
        }

        public Request(String[] names, IndicesOptions indicesOptions) {
            this(names, indicesOptions, false);
        }

        public Request(String[] names, IndicesOptions indicesOptions, boolean includeResolvedInResponse) {
            this.names = names;
            this.indicesOptions = indicesOptions;
            this.rewrittenExpressions = null;
            this.includeResolvedInResponse = includeResolvedInResponse;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.names = in.readStringArray();
            this.indicesOptions = IndicesOptions.readIndicesOptions(in);
            this.rewrittenExpressions = null;
            // skipping BWC handling here
            this.includeResolvedInResponse = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(names);
            indicesOptions.writeIndicesOptions(out);
            // skipping BWC handling here
            out.writeBoolean(includeResolvedInResponse);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Arrays.equals(names, request.names);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(names);
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
        public void setRewrittenExpressions(List<RewrittenExpression> rewrittenExpressions) {
            this.rewrittenExpressions = rewrittenExpressions;
            indices(
                rewrittenExpressions.stream()
                    .flatMap(indexExpression -> indexExpression.canonicalExpressions().stream().map(CanonicalExpression::expression))
                    .toArray(String[]::new)
            );
        }

        @Override
        public List<RewrittenExpression> getRewrittenExpressions() {
            return rewrittenExpressions;
        }

        @Override
        public boolean enabled() {
            return includeResolvedInResponse;
        }

        @Override
        public Map<String, List<String>> resolved() {
            return resolved;
        }

        @Override
        public void resolved(Map<String, List<String>> resolved) {
            assert enabled();
            this.resolved = resolved;
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
            if (in.getTransportVersion().onOrAfter(TransportVersions.RESOLVE_INDEX_MODE_ADDED)) {
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
            if (out.getTransportVersion().onOrAfter(TransportVersions.RESOLVE_INDEX_MODE_ADDED)) {
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
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        static final ParseField INDICES_FIELD = new ParseField("indices");
        static final ParseField ALIASES_FIELD = new ParseField("aliases");
        static final ParseField DATA_STREAMS_FIELD = new ParseField("data_streams");
        static final ParseField RESOLVED = new ParseField("resolved");

        private final List<ResolvedIndex> indices;
        private final List<ResolvedAlias> aliases;
        private final List<ResolvedDataStream> dataStreams;
        private final Map<String, List<String>> resolved;

        public Response(List<ResolvedIndex> indices, List<ResolvedAlias> aliases, List<ResolvedDataStream> dataStreams) {
            this(indices, aliases, dataStreams, Map.of());
        }

        public Response(
            List<ResolvedIndex> indices,
            List<ResolvedAlias> aliases,
            List<ResolvedDataStream> dataStreams,
            Map<String, List<String>> resolved
        ) {
            this.indices = indices;
            this.aliases = aliases;
            this.dataStreams = dataStreams;
            this.resolved = resolved == null ? Map.of() : resolved;
        }

        public Response(StreamInput in) throws IOException {
            this.indices = in.readCollectionAsList(ResolvedIndex::new);
            this.aliases = in.readCollectionAsList(ResolvedAlias::new);
            this.dataStreams = in.readCollectionAsList(ResolvedDataStream::new);
            this.resolved = in.readMap(StreamInput::readString, StreamInput::readStringCollectionAsImmutableList);
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

        public Map<String, List<String>> getResolved() {
            return resolved;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(indices);
            out.writeCollection(aliases);
            out.writeCollection(dataStreams);
            out.writeMap(resolved, StreamOutput::writeString, StreamOutput::writeStringCollection);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.xContentList(INDICES_FIELD.getPreferredName(), indices);
            builder.xContentList(ALIASES_FIELD.getPreferredName(), aliases);
            builder.xContentList(DATA_STREAMS_FIELD.getPreferredName(), dataStreams);
            if (false == resolved.isEmpty()) {
                builder.startObject(RESOLVED.getPreferredName());
                for (Map.Entry<String, List<String>> entry : resolved.entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
                builder.endObject();
            }
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
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private static final Logger logger = LogManager.getLogger(TransportAction.class);

        private final ClusterService clusterService;
        private final RemoteClusterService remoteClusterService;
        private final ProjectResolver projectResolver;
        private final IndexNameExpressionResolver indexNameExpressionResolver;
        private final boolean ccsCheckCompatibility;

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ActionFilters actionFilters,
            ProjectResolver projectResolver,
            IndexNameExpressionResolver indexNameExpressionResolver
        ) {
            super(NAME, transportService, actionFilters, Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
            this.clusterService = clusterService;
            this.remoteClusterService = transportService.getRemoteClusterService();
            this.projectResolver = projectResolver;
            this.indexNameExpressionResolver = indexNameExpressionResolver;
            this.ccsCheckCompatibility = SearchService.CCS_VERSION_CHECK_SETTING.get(clusterService.getSettings());
        }

        @Override
        protected void doExecute(Task task, Request request, final ActionListener<Response> listener) {
            if (ccsCheckCompatibility) {
                checkCCSVersionCompatibility(request);
            }
            final boolean crossProjectModeEnabled = request.crossProjectModeEnabled();

            final ProjectState projectState = projectResolver.getProjectState(clusterService.state());
            final Map<String, OriginalIndices> remoteClusterIndices = remoteClusterService.groupIndices(
                request.indicesOptions(),
                request.indices()
            );

            final OriginalIndices localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            logger.info("Original local indices [{}]", localIndices);

            if (request.includeResolvedInResponse) {
                assert false == crossProjectModeEnabled : "includeUnresolvedInResponse should not be used in cross-project mode";
                assert remoteClusterIndices.isEmpty() : "includeUnresolvedInResponse should not be used with remote clusters";
            }

            List<ResolvedIndex> indices = new ArrayList<>();
            List<ResolvedAlias> aliases = new ArrayList<>();
            List<ResolvedDataStream> dataStreams = new ArrayList<>();
            if (crossProjectModeEnabled) {
                if (localIndices != null) {
                    resolveIndices(
                        localIndices.indices(),
                        lenientIndicesOptions(localIndices.indicesOptions()),
                        projectState,
                        indexNameExpressionResolver,
                        indices,
                        aliases,
                        dataStreams
                    );
                }
            } else {
                resolveIndices(localIndices, projectState, indexNameExpressionResolver, indices, aliases, dataStreams);
            }

            if (false == remoteClusterIndices.isEmpty()) {
                final int remoteRequests = remoteClusterIndices.size();
                final CountDown completionCounter = new CountDown(remoteRequests);
                final SortedMap<String, Response> remoteResponses = Collections.synchronizedSortedMap(new TreeMap<>());
                final Runnable terminalHandler = () -> {
                    if (completionCounter.countDown()) {
                        if (crossProjectModeEnabled) {
                            try {
                                maybeThrowForFlatWorld(request, remoteResponses);
                            } catch (Exception ex) {
                                logger.warn("Failed to resolve indices in flat world mode", ex);
                                listener.onFailure(ex);
                                return;
                            }
                        }
                        mergeResults(remoteResponses, indices, aliases, dataStreams);
                        listener.onResponse(new Response(indices, aliases, dataStreams, request.resolved()));
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
                    Request remoteRequest;
                    if (crossProjectModeEnabled) {
                        // we need a lenient request because we can't throw exceptions just yet
                        remoteRequest = new Request(
                            originalIndices.indices(),
                            lenientIndicesOptions(originalIndices.indicesOptions()),
                            true
                        );
                    } else {
                        remoteRequest = new Request(originalIndices.indices(), originalIndices.indicesOptions(), false);
                    }
                    remoteClusterClient.execute(ResolveIndexAction.REMOTE_TYPE, remoteRequest, ActionListener.wrap(response -> {
                        remoteResponses.put(clusterAlias, response);
                        terminalHandler.run();
                    }, failure -> terminalHandler.run()));
                }
            } else {
                listener.onResponse(new Response(indices, aliases, dataStreams, request.resolved()));
            }
        }

        private static IndicesOptions lenientIndicesOptions(IndicesOptions indicesOptions) {
            return IndicesOptions.builder(indicesOptions)
                .concreteTargetOptions(new IndicesOptions.ConcreteTargetOptions(true))
                .wildcardOptions(
                    IndicesOptions.WildcardOptions.builder(indicesOptions.wildcardOptions()).allowEmptyExpressions(true).build()
                )
                .build();
        }

        void maybeThrowForFlatWorld(IndicesRequest.CrossProjectResolvable resolvable, Map<String, Response> remoteResponses) {
            logger.info("Checking if we should throw in flat world");
            if (resolvable.indicesOptions().allowNoIndices() && resolvable.indicesOptions().ignoreUnavailable()) {
                // nothing to do since we're in lenient mode
                logger.info("Skipping index existence check in lenient mode");
                return;
            }

            List<IndicesRequest.RewrittenExpression> rewrittenExpressions = resolvable.getRewrittenExpressions();
            assert rewrittenExpressions != null;
            for (IndicesRequest.RewrittenExpression rewrittenExpression : rewrittenExpressions) {
                // TODO need to handle qualified expressions here, too
                var original = rewrittenExpression.original();
                boolean exists = rewrittenExpression.hasCanonicalExpressionForOrigin();
                if (exists) {
                    logger.info("Local cluster has canonical expression for [{}], skipping remote existence check", original);
                    continue;
                }

                for (var remoteResponse : remoteResponses.values()) {
                    Map<String, List<String>> resolved = remoteResponse.getResolved();
                    assert resolved != null;
                    if (resolved.containsKey(original) && resolved.get(original).isEmpty() == false) {
                        logger.info("Remote cluster has resolved entries for [{}], skipping further remote existence check", original);
                        exists = true;
                        break;
                    }
                }

                if (false == exists && false == resolvable.indicesOptions().ignoreUnavailable()) {
                    throw new IndexNotFoundException(original);
                }
            }
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
            List<ResolvedDataStream> dataStreams
        ) {
            if (localIndices == null) {
                return;
            }
            resolveIndices(localIndices.indices(), localIndices.indicesOptions(), projectState, resolver, indices, aliases, dataStreams);
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
            List<ResolvedDataStream> dataStreams
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
                enrichIndexAbstraction(projectState, s, indices, aliases, dataStreams);
            }
            indices.sort(Comparator.comparing(ResolvedIndexAbstraction::getName));
            aliases.sort(Comparator.comparing(ResolvedIndexAbstraction::getName));
            dataStreams.sort(Comparator.comparing(ResolvedIndexAbstraction::getName));
        }

        private static void mergeResults(
            Map<String, Response> remoteResponses,
            List<ResolvedIndex> indices,
            List<ResolvedAlias> aliases,
            List<ResolvedDataStream> dataStreams
        ) {
            for (Map.Entry<String, Response> responseEntry : remoteResponses.entrySet()) {
                String clusterAlias = responseEntry.getKey();
                Response response = responseEntry.getValue();
                logger.info("Cluster alias [{}] has resolved entries [{}]", clusterAlias, response.getResolved());

                for (ResolvedIndex index : response.indices) {
                    indices.add(index.copy(RemoteClusterAware.buildRemoteIndexName(clusterAlias, index.getName())));
                }
                for (ResolvedAlias alias : response.aliases) {
                    aliases.add(alias.copy(RemoteClusterAware.buildRemoteIndexName(clusterAlias, alias.getName())));
                }
                for (ResolvedDataStream dataStream : response.dataStreams) {
                    dataStreams.add(dataStream.copy(RemoteClusterAware.buildRemoteIndexName(clusterAlias, dataStream.getName())));
                }
            }
        }

        private static void enrichIndexAbstraction(
            ProjectState projectState,
            ResolvedExpression resolvedExpression,
            List<ResolvedIndex> indices,
            List<ResolvedAlias> aliases,
            List<ResolvedDataStream> dataStreams
        ) {
            SortedMap<String, IndexAbstraction> indicesLookup = projectState.metadata().getIndicesLookup();
            IndexAbstraction ia = indicesLookup.get(resolvedExpression.resource());
            if (ia != null) {
                switch (ia.getType()) {
                    case CONCRETE_INDEX -> {
                        IndexMetadata writeIndex = projectState.metadata().index(ia.getWriteIndex());
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
                                writeIndex.getIndexMode() == null ? IndexMode.STANDARD : writeIndex.getIndexMode()
                            )
                        );
                    }
                    case ALIAS -> {
                        String[] indexNames = getAliasIndexStream(resolvedExpression, ia, projectState.metadata()).map(Index::getName)
                            .toArray(String[]::new);
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
                        String[] backingIndices = dataStreamIndices.map(Index::getName).toArray(String[]::new);
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
