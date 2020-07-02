/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.resolve;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.stream.StreamSupport;

public class ResolveIndexAction extends ActionType<ResolveIndexAction.Response> {

    public static final ResolveIndexAction INSTANCE = new ResolveIndexAction();
    public static final String NAME = "indices:admin/resolve/index";

    private ResolveIndexAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest implements IndicesRequest.Replaceable {

        public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.strictExpandOpen();

        private String[] names;
        private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;

        public Request(String[] names) {
            this.names = names;
        }

        public Request(String[] names, IndicesOptions indicesOptions) {
            this.names = names;
            this.indicesOptions = indicesOptions;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.names = in.readStringArray();
            this.indicesOptions = IndicesOptions.readIndicesOptions(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(names);
            indicesOptions.writeIndicesOptions(out);
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

        private final String[] aliases;
        private final String[] attributes;
        private final String dataStream;

        ResolvedIndex(StreamInput in) throws IOException {
            setName(in.readString());
            this.aliases = in.readStringArray();
            this.attributes = in.readStringArray();
            this.dataStream = in.readOptionalString();
        }

        ResolvedIndex(String name, String[] aliases, String[] attributes, @Nullable String dataStream) {
            super(name);
            this.aliases = aliases;
            this.attributes = attributes;
            this.dataStream = dataStream;
        }

        public ResolvedIndex copy(String newName) {
            return new ResolvedIndex(newName, aliases, attributes, dataStream);
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

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(getName());
            out.writeStringArray(aliases);
            out.writeStringArray(attributes);
            out.writeOptionalString(dataStream);
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
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ResolvedIndex index = (ResolvedIndex) o;
            return getName().equals(index.getName()) && Objects.equals(dataStream, index.dataStream) &&
                Arrays.equals(aliases, index.aliases) && Arrays.equals(attributes, index.attributes);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(getName(), dataStream);
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
            return getName().equals(dataStream.getName()) && timestampField.equals(dataStream.timestampField) &&
                Arrays.equals(backingIndices, dataStream.backingIndices);
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

        private final List<ResolvedIndex> indices;
        private final List<ResolvedAlias> aliases;
        private final List<ResolvedDataStream> dataStreams;

        public Response(List<ResolvedIndex> indices, List<ResolvedAlias> aliases, List<ResolvedDataStream> dataStreams) {
            this.indices = indices;
            this.aliases = aliases;
            this.dataStreams = dataStreams;
        }

        public Response(StreamInput in) throws IOException {
            this.indices = new ArrayList<>();
            int count = in.readVInt();
            for (int k = 0; k < count; k++) {
                indices.add(new ResolvedIndex(in));
            }
            this.aliases = new ArrayList<>();
            count = in.readVInt();
            for (int k = 0; k < count; k++) {
                aliases.add(new ResolvedAlias(in));
            }
            this.dataStreams = new ArrayList<>();
            count = in.readVInt();
            for (int k = 0; k < count; k++) {
                dataStreams.add(new ResolvedDataStream(in));
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
            out.writeVInt(indices.size());
            for (ResolvedIndex index : indices) {
                index.writeTo(out);
            }
            out.writeVInt(aliases.size());
            for (ResolvedAlias alias : aliases) {
                alias.writeTo(out);
            }
            out.writeVInt(dataStreams.size());
            for (ResolvedDataStream dataStream : dataStreams) {
                dataStream.writeTo(out);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(INDICES_FIELD.getPreferredName(), indices);
            builder.field(ALIASES_FIELD.getPreferredName(), aliases);
            builder.field(DATA_STREAMS_FIELD.getPreferredName(), dataStreams);
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

        private final ThreadPool threadPool;
        private final ClusterService clusterService;
        private final RemoteClusterService remoteClusterService;
        private final IndexNameExpressionResolver indexNameExpressionResolver;

        @Inject
        public TransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
            super(NAME, transportService, actionFilters, Request::new);
            this.threadPool = threadPool;
            this.clusterService = clusterService;
            this.remoteClusterService = transportService.getRemoteClusterService();
            this.indexNameExpressionResolver = indexNameExpressionResolver;
        }

        @Override
        protected void doExecute(Task task, Request request, final ActionListener<Response> listener) {
            final ClusterState clusterState = clusterService.state();
            final Map<String, OriginalIndices> remoteClusterIndices = remoteClusterService.groupIndices(request.indicesOptions(),
                request.indices());
            final OriginalIndices localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            final Metadata metadata = clusterState.metadata();
            List<ResolvedIndex> indices = new ArrayList<>();
            List<ResolvedAlias> aliases = new ArrayList<>();
            List<ResolvedDataStream> dataStreams = new ArrayList<>();
            if (localIndices != null) {
                resolveIndices(localIndices.indices(), request.indicesOptions, metadata, indexNameExpressionResolver, indices, aliases,
                    dataStreams);
            }

            if (remoteClusterIndices.size() > 0) {
                final int remoteRequests = remoteClusterIndices.size();
                final CountDown completionCounter = new CountDown(remoteRequests);
                final SortedMap<String, Response> remoteResponses = Collections.synchronizedSortedMap(new TreeMap<>());
                final Runnable terminalHandler = () -> {
                    if (completionCounter.countDown()) {
                        mergeResults(remoteResponses, indices, aliases, dataStreams);
                        listener.onResponse(new Response(indices, aliases, dataStreams));
                    }
                };

                // make the cross-cluster calls
                for (Map.Entry<String, OriginalIndices> remoteIndices : remoteClusterIndices.entrySet()) {
                    String clusterAlias = remoteIndices.getKey();
                    OriginalIndices originalIndices = remoteIndices.getValue();
                    Client remoteClusterClient = remoteClusterService.getRemoteClusterClient(threadPool, clusterAlias);
                    Request remoteRequest = new Request(originalIndices.indices(), originalIndices.indicesOptions());
                    remoteClusterClient.admin().indices().resolveIndex(remoteRequest, ActionListener.wrap(response -> {
                        remoteResponses.put(clusterAlias, response);
                        terminalHandler.run();
                    }, failure -> terminalHandler.run()));
                }
            } else {
                listener.onResponse(new Response(indices, aliases, dataStreams));
            }
        }

        /**
         * Resolves the specified names and/or wildcard expressions to index abstractions. Returns results in the supplied lists.
         *
         * @param names          The names and wildcard expressions to resolve
         * @param indicesOptions Options for expanding wildcards to indices with different states
         * @param metadata       Cluster metadata
         * @param resolver       Resolver instance for matching names
         * @param indices        List containing any matching indices
         * @param aliases        List containing any matching aliases
         * @param dataStreams    List containing any matching data streams
         */
        // visible for testing
        static void resolveIndices(String[] names, IndicesOptions indicesOptions, Metadata metadata, IndexNameExpressionResolver resolver,
                                   List<ResolvedIndex> indices, List<ResolvedAlias> aliases, List<ResolvedDataStream> dataStreams) {
            List<String> resolvedIndexAbstractions = resolveIndexAbstractions(names, indicesOptions, metadata, resolver);
            SortedMap<String, IndexAbstraction> lookup = metadata.getIndicesLookup();
            for (String s : resolvedIndexAbstractions) {
                enrichIndexAbstraction(s, lookup, indices, aliases, dataStreams);
            }
            indices.sort(Comparator.comparing(ResolvedIndexAbstraction::getName));
            aliases.sort(Comparator.comparing(ResolvedIndexAbstraction::getName));
            dataStreams.sort(Comparator.comparing(ResolvedIndexAbstraction::getName));

        }

        private static void mergeResults(Map<String, Response> remoteResponses, List<ResolvedIndex> indices, List<ResolvedAlias> aliases,
                                         List<ResolvedDataStream> dataStreams) {
            for (Map.Entry<String, Response> responseEntry: remoteResponses.entrySet()) {
                String clusterAlias = responseEntry.getKey();
                Response response = responseEntry.getValue();
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

        private static List<String> resolveIndexAbstractions(String[] indices, IndicesOptions indicesOptions, Metadata metadata,
                                                             IndexNameExpressionResolver indexNameExpressionResolver) {
            final boolean replaceWildcards = indicesOptions.expandWildcardsOpen() || indicesOptions.expandWildcardsClosed();
            Set<String> availableIndexAbstractions = metadata.getIndicesLookup().keySet();
            List<String> finalIndices = new ArrayList<>();
            boolean wildcardSeen = false;
            for (String index : indices) {
                String indexAbstraction;
                boolean minus = false;
                if (index.charAt(0) == '-' && wildcardSeen) {
                    indexAbstraction = index.substring(1);
                    minus = true;
                } else {
                    indexAbstraction = index;
                }

                // we always need to check for date math expressions
                final String dateMathName = indexNameExpressionResolver.resolveDateMathExpression(indexAbstraction);
                if (dateMathName != indexAbstraction) {
                    assert dateMathName.equals(indexAbstraction) == false;
                    if (replaceWildcards && Regex.isSimpleMatchPattern(dateMathName)) {
                        // continue
                        indexAbstraction = dateMathName;
                    } else if (availableIndexAbstractions.contains(dateMathName) &&
                        isIndexVisible(indexAbstraction, dateMathName, indicesOptions, metadata, true)) {
                        if (minus) {
                            finalIndices.remove(dateMathName);
                        } else {
                            finalIndices.add(dateMathName);
                        }
                    } else {
                        if (indicesOptions.ignoreUnavailable() == false) {
                            throw new IndexNotFoundException(dateMathName);
                        }
                    }
                }

                if (replaceWildcards && Regex.isSimpleMatchPattern(indexAbstraction)) {
                    wildcardSeen = true;
                    Set<String> resolvedIndices = new HashSet<>();
                    for (String authorizedIndex : availableIndexAbstractions) {
                        if (Regex.simpleMatch(indexAbstraction, authorizedIndex) &&
                            isIndexVisible(indexAbstraction, authorizedIndex, indicesOptions, metadata)) {
                            resolvedIndices.add(authorizedIndex);
                        }
                    }
                    if (resolvedIndices.isEmpty()) {
                        //es core honours allow_no_indices for each wildcard expression, we do the same here by throwing index not found.
                        if (indicesOptions.allowNoIndices() == false) {
                            throw new IndexNotFoundException(indexAbstraction);
                        }
                    } else {
                        if (minus) {
                            finalIndices.removeAll(resolvedIndices);
                        } else {
                            finalIndices.addAll(resolvedIndices);
                        }
                    }
                } else if (dateMathName.equals(indexAbstraction)) {
                    if (minus) {
                        finalIndices.remove(indexAbstraction);
                    } else {
                        finalIndices.add(indexAbstraction);
                    }
                }
            }
            return finalIndices;
        }

        private static boolean isIndexVisible(String expression, String index, IndicesOptions indicesOptions, Metadata metadata) {
            return isIndexVisible(expression, index, indicesOptions, metadata, false);
        }

        private static boolean isIndexVisible(String expression, String index, IndicesOptions indicesOptions, Metadata metadata,
                                              boolean dateMathExpression) {
            IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(index);
            final boolean isHidden = indexAbstraction.isHidden();
            if (indexAbstraction.getType() == IndexAbstraction.Type.ALIAS) {
                //it's an alias, ignore expandWildcardsOpen and expandWildcardsClosed.
                //complicated to support those options with aliases pointing to multiple indices...
                if (indicesOptions.ignoreAliases()) {
                    return false;
                } else if (isHidden == false || indicesOptions.expandWildcardsHidden() || isVisibleDueToImplicitHidden(expression, index)) {
                    return true;
                } else {
                    return false;
                }
            }
            if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM) {
                // If indicesOptions.includeDataStreams() returns false then we fail later in IndexNameExpressionResolver.
                if (isHidden == false || indicesOptions.expandWildcardsHidden()) {
                    return true;
                } else {
                    return false;
                }
            }
            assert indexAbstraction.getIndices().size() == 1 : "concrete index must point to a single index";
            IndexMetadata indexMetadata = indexAbstraction.getIndices().get(0);
            if (isHidden && indicesOptions.expandWildcardsHidden() == false && isVisibleDueToImplicitHidden(expression, index) == false) {
                return false;
            }

            // the index is not hidden and since it is a date math expression, we consider it visible regardless of open/closed
            if (dateMathExpression) {
                assert IndexMetadata.State.values().length == 2 : "a new IndexMetadata.State value may need to be handled!";
                return true;
            }
            if (indexMetadata.getState() == IndexMetadata.State.CLOSE && indicesOptions.expandWildcardsClosed()) {
                return true;
            }
            if (indexMetadata.getState() == IndexMetadata.State.OPEN && indicesOptions.expandWildcardsOpen()) {
                return true;
            }
            return false;
        }

        private static boolean isVisibleDueToImplicitHidden(String expression, String index) {
            return index.startsWith(".") && expression.startsWith(".") && Regex.isSimpleMatchPattern(expression);
        }

        private static void enrichIndexAbstraction(String indexAbstraction, SortedMap<String, IndexAbstraction> lookup,
                                                   List<ResolvedIndex> indices, List<ResolvedAlias> aliases,
                                                   List<ResolvedDataStream> dataStreams) {
            IndexAbstraction ia = lookup.get(indexAbstraction);
            if (ia != null) {
                switch (ia.getType()) {
                    case CONCRETE_INDEX:
                        IndexAbstraction.Index index = (IndexAbstraction.Index) ia;

                        String[] aliasNames = StreamSupport.stream(
                            Spliterators.spliteratorUnknownSize(index.getWriteIndex().getAliases().keysIt(), 0), false)
                            .toArray(String[]::new);
                        Arrays.sort(aliasNames);

                        List<String> attributes = new ArrayList<>();
                        attributes.add(index.getWriteIndex().getState() == IndexMetadata.State.OPEN ? "open" : "closed");
                        if (ia.isHidden()) {
                            attributes.add("hidden");
                        }
                        final boolean isFrozen = Boolean.parseBoolean(ia.getWriteIndex().getSettings().get("index.frozen"));
                        if (isFrozen) {
                            attributes.add("frozen");
                        }
                        attributes.sort(String::compareTo);

                        indices.add(new ResolvedIndex(
                            index.getName(),
                            aliasNames,
                            attributes.toArray(Strings.EMPTY_ARRAY),
                            index.getParentDataStream() == null ? null : index.getParentDataStream().getName()));
                        break;
                    case ALIAS:
                        IndexAbstraction.Alias alias = (IndexAbstraction.Alias) ia;
                        String[] indexNames = alias.getIndices().stream().map(i -> i.getIndex().getName()).toArray(String[]::new);
                        Arrays.sort(indexNames);
                        aliases.add(new ResolvedAlias(alias.getName(), indexNames));
                        break;
                    case DATA_STREAM:
                        IndexAbstraction.DataStream dataStream = (IndexAbstraction.DataStream) ia;
                        String[] backingIndices = dataStream.getIndices().stream().map(i -> i.getIndex().getName()).toArray(String[]::new);
                        Arrays.sort(backingIndices);
                        dataStreams.add(new ResolvedDataStream(
                            dataStream.getName(),
                            backingIndices,
                            dataStream.getDataStream().getTimeStampField().getName()));
                        break;
                    default:
                        throw new IllegalStateException("unknown index abstraction type: " + ia.getType());
                }
            }
        }
    }
}
