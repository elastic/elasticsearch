/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Streams;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.CharBuffer;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.entsearch.engine.Engine.BINARY_CONTENT_FIELD;
import static org.elasticsearch.xpack.entsearch.engine.Engine.INDICES_FIELD;
import static org.elasticsearch.xpack.entsearch.engine.Engine.NAME_FIELD;

/**
 * A service that manages the persistent {@link Engine} configurations.
 *
 * TODO: Revise the internal format (mappings). Should we use rest or transport versioning for BWC?
 */
public class EngineIndexService {
    private static final Logger logger = LogManager.getLogger(EngineIndexService.class);
    public static final String ENGINE_ALIAS_NAME = ".engine";
    public static final String ENGINE_CONCRETE_INDEX_NAME = ".engine-1";
    public static final String ENGINE_INDEX_NAME_PATTERN = ".engine-*";

    private final Client clientWithOrigin;
    private final ClusterService clusterService;
    public final NamedWriteableRegistry namedWriteableRegistry;
    private final BigArrays bigArrays;

    public EngineIndexService(
        Client client,
        ClusterService clusterService,
        NamedWriteableRegistry namedWriteableRegistry,
        BigArrays bigArrays
    ) {
        this.clientWithOrigin = new OriginSettingClient(client, ENT_SEARCH_ORIGIN);
        this.clusterService = clusterService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.bigArrays = bigArrays;
    }

    /**
     * Returns the {@link SystemIndexDescriptor} for the {@link Engine} system index.
     *
     * @return The {@link SystemIndexDescriptor} for the {@link Engine} system index.
     */
    public static SystemIndexDescriptor getSystemIndexDescriptor() {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(ENGINE_INDEX_NAME_PATTERN)
            .setPrimaryIndex(ENGINE_CONCRETE_INDEX_NAME)
            .setDescription("Contains Engine configuration")
            .setMappings(getIndexMappings())
            .setSettings(getIndexSettings())
            .setAliasName(ENGINE_ALIAS_NAME)
            .setVersionMetaKey("version")
            .setOrigin(ENT_SEARCH_ORIGIN)
            .setThreadPools(ExecutorNames.DEFAULT_SYSTEM_INDEX_THREAD_POOLS)
            .build();
    }

    private static Settings getIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .put(IndexMetadata.SETTING_PRIORITY, 100)
            .put("index.refresh_interval", "1s")
            .build();
    }

    private static XContentBuilder getIndexMappings() {
        try {
            final XContentBuilder builder = jsonBuilder();
            builder.startObject();
            {
                builder.startObject("_meta");
                builder.field("version", Version.CURRENT.toString());
                builder.endObject();

                builder.field("dynamic", "strict");
                builder.startObject("properties");
                {
                    builder.startObject(NAME_FIELD.getPreferredName());
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject(INDICES_FIELD.getPreferredName());
                    builder.field("type", "keyword");
                    builder.endObject();

                    builder.startObject(BINARY_CONTENT_FIELD.getPreferredName());
                    builder.field("type", "object");
                    builder.field("enabled", "false");
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        } catch (IOException e) {
            logger.fatal("Failed to build " + ENGINE_CONCRETE_INDEX_NAME + " index mappings", e);
            throw new UncheckedIOException("Failed to build " + ENGINE_CONCRETE_INDEX_NAME + " index mappings", e);
        }
    }

    /**
     * Gets the {@link Engine} from the index if present, or delegate a {@link ResourceNotFoundException} failure to the provided
     * listener if not.
     *
     * @param resourceName The resource name.
     * @param listener The action listener to invoke on response/failure.
     */
    public void getEngine(String resourceName, ActionListener<Engine> listener) {
        final GetRequest getRequest = new GetRequest(ENGINE_ALIAS_NAME).id(resourceName).realtime(true);
        clientWithOrigin.get(getRequest, listener.delegateFailure((delegate, getResponse) -> {
            if (getResponse.isExists() == false) {
                delegate.onFailure(new ResourceNotFoundException(resourceName));
                return;
            }
            final BytesReference source = getResponse.getSourceInternal();
            final Engine res = parseEngineBinaryFromSource(source);
            delegate.onResponse(res);
        }));
    }

    private static String getEngineAliasName(Engine engine) {
        return engine.engineAlias();
    }

    /**
     * Creates or updates the {@link Engine} in the underlying index.
     *
     * @param engine The engine object.
     * @param listener The action listener to invoke on response/failure.
     */
    public void putEngine(Engine engine, ActionListener<IndexResponse> listener) {
        createOrUpdateAlias(engine, new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                updateEngine(engine, listener);
            }

            @Override
            public void onFailure(Exception e) {
                // Convert index not found failure from the alias API into an illegal argument
                Exception failException = e;
                if (e instanceof IndexNotFoundException) {
                    failException = new IllegalArgumentException(e.getMessage(), e);
                }
                listener.onFailure(failException);
            }
        });
    }

    private void createOrUpdateAlias(Engine engine, ActionListener<AcknowledgedResponse> listener) {

        final Metadata metadata = clusterService.state().metadata();
        final String engineAliasName = getEngineAliasName(engine);

        IndicesAliasesRequestBuilder requestBuilder = null;
        if (metadata.hasAlias(engineAliasName)) {
            Set<String> currentAliases = metadata.aliasedIndices(engineAliasName)
                .stream()
                .map(index -> index.getName())
                .collect(Collectors.toSet());
            Set<String> targetAliases = Set.of(engine.indices());

            requestBuilder = updateAliasIndices(currentAliases, targetAliases, engineAliasName);

        } else {
            requestBuilder = clientWithOrigin.admin().indices().prepareAliases().addAlias(engine.indices(), engineAliasName);
        }

        requestBuilder.execute(listener);
    }

    private IndicesAliasesRequestBuilder updateAliasIndices(Set<String> currentAliases, Set<String> targetAliases, String engineAliasName) {

        Set<String> deleteIndices = new HashSet<>(currentAliases);
        deleteIndices.removeAll(targetAliases);

        IndicesAliasesRequestBuilder aliasesRequestBuilder = clientWithOrigin.admin().indices().prepareAliases();

        // Always re-add aliases, as an index could have been removed manually and it must be restored
        for (String newIndex : targetAliases) {
            aliasesRequestBuilder.addAliasAction(IndicesAliasesRequest.AliasActions.add().index(newIndex).alias(engineAliasName));
        }
        for (String deleteIndex : deleteIndices) {
            aliasesRequestBuilder.addAliasAction(IndicesAliasesRequest.AliasActions.remove().index(deleteIndex).alias(engineAliasName));
        }

        return aliasesRequestBuilder;
    }

    private void updateEngine(Engine engine, ActionListener<IndexResponse> listener) {
        try (ReleasableBytesStreamOutput buffer = new ReleasableBytesStreamOutput(0, bigArrays.withCircuitBreaking())) {
            try (XContentBuilder source = XContentFactory.jsonBuilder(buffer)) {
                source.startObject()
                    .field(NAME_FIELD.getPreferredName(), engine.name())
                    .field(INDICES_FIELD.getPreferredName(), engine.indices())
                    .directFieldAsBase64(
                        BINARY_CONTENT_FIELD.getPreferredName(),
                        os -> writeEngineBinaryWithVersion(engine, os, clusterService.state().nodes().getMinNodeVersion())
                    )
                    .endObject();
            }
            final IndexRequest indexRequest = new IndexRequest(ENGINE_ALIAS_NAME).opType(DocWriteRequest.OpType.INDEX)
                .id(engine.name())
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(buffer.bytes(), XContentType.JSON);
            clientWithOrigin.index(indexRequest, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Deletes the provided {@param engineName} in the underlying index, or delegate a failure to the provided
     * listener if the resource does not exist or failed to delete.
     *
     * @param engineName The name of the {@link Engine} to delete.
     * @param listener The action listener to invoke on response/failure.
     *
     */
    public void deleteEngine(String engineName, ActionListener<DeleteResponse> listener) {
        try {
            // TODO Delete alias when Engine is deleted
            final DeleteRequest deleteRequest = new DeleteRequest(ENGINE_ALIAS_NAME).id(engineName)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            clientWithOrigin.delete(deleteRequest, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * List the {@link Engine} in ascending order of their names.
     *
     * @param queryString The query string to filter the results.
     * @param from From index to start the search from.
     * @param size The maximum number of {@link Engine} to return.
     * @param listener The action listener to invoke on response/failure.
     */
    public void listEngine(String queryString, int from, int size, ActionListener<SearchResponse> listener) {
        try {
            final SearchSourceBuilder source = new SearchSourceBuilder().from(from)
                .size(size)
                .query(new QueryStringQueryBuilder(queryString))
                .docValueField(NAME_FIELD.getPreferredName())
                .docValueField(INDICES_FIELD.getPreferredName())
                .storedFields(Collections.singletonList("_none_"))
                .sort(NAME_FIELD.getPreferredName(), SortOrder.ASC);
            final SearchRequest req = new SearchRequest(ENGINE_ALIAS_NAME).source(source);
            clientWithOrigin.search(req, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private Engine parseEngineBinaryFromSource(BytesReference source) {
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, XContentType.JSON)) {
            ensureExpectedToken(parser.nextToken(), XContentParser.Token.START_OBJECT, parser);
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
                parser.nextToken();
                if (BINARY_CONTENT_FIELD.getPreferredName().equals(parser.currentName())) {
                    final CharBuffer encodedBuffer = parser.charBuffer();
                    InputStream encodedIn = Base64.getDecoder().wrap(new InputStream() {
                        @Override
                        public int read() {
                            if (encodedBuffer.hasRemaining()) {
                                return encodedBuffer.get();
                            } else {
                                return -1; // end of stream
                            }
                        }
                    });
                    try (
                        StreamInput in = new NamedWriteableAwareStreamInput(new InputStreamStreamInput(encodedIn), namedWriteableRegistry)
                    ) {
                        return parseEngineBinaryWithVersion(in);
                    }
                } else {
                    XContentParserUtils.parseFieldsValue(parser); // consume and discard unknown fields
                }
            }
            throw new ElasticsearchParseException("[" + BINARY_CONTENT_FIELD.getPreferredName() + "] field is missing");
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse: " + source.utf8ToString(), e);
        }
    }

    static Engine parseEngineBinaryWithVersion(StreamInput in) throws IOException {
        TransportVersion version = TransportVersion.readVersion(in);
        assert version.onOrBefore(TransportVersion.CURRENT) : version + " >= " + TransportVersion.CURRENT;
        in.setTransportVersion(version);
        return new Engine(in);
    }

    static void writeEngineBinaryWithVersion(Engine engine, OutputStream os, Version minNodeVersion) throws IOException {
        // do not close the output
        os = Streams.noCloseStream(os);
        TransportVersion.writeVersion(minNodeVersion.transportVersion, new OutputStreamStreamOutput(os));
        try (OutputStreamStreamOutput out = new OutputStreamStreamOutput(os)) {
            out.setTransportVersion(minNodeVersion.transportVersion);
            engine.writeTo(out);
        }
    }
}
