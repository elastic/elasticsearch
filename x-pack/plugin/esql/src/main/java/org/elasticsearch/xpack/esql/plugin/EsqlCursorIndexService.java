/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Streams;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.CharBuffer;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.ESQL_ORIGIN;

/**
 * Stores and retrieves paginated ES|QL cursor state in a system index ({@code .esql-cursors}).
 * <p>
 * Each cursor is stored as multiple documents: one metadata document containing column schema
 * and cursor-level settings, plus one page document per output page containing the serialized
 * row data. This allows fetching a single page without deserializing the entire result set.
 */
public class EsqlCursorIndexService {

    private static final Logger logger = LogManager.getLogger(EsqlCursorIndexService.class);

    public static final String CURSOR_INDEX = ".esql-cursors";
    public static final TimeValue DEFAULT_CURSOR_KEEP_ALIVE = TimeValue.timeValueMinutes(5);

    static final String DOC_TYPE_FIELD = "doc_type";
    static final String CURSOR_ID_FIELD = "cursor_id";
    static final String EXPIRATION_TIME_FIELD = "expiration_time";
    static final String COLUMNS_FIELD = "columns";
    static final String TOTAL_ROWS_FIELD = "total_rows";
    static final String TOTAL_PAGES_FIELD = "total_pages";
    /** Sentinel value for {@code total_pages} indicating the query is still producing pages. */
    static final int TOTAL_PAGES_IN_PROGRESS = -1;
    /** Sentinel value for {@code total_pages} indicating the query failed after partial storage. */
    static final int TOTAL_PAGES_FAILED = -2;
    static final String ZONE_ID_FIELD = "zone_id";
    static final String COLUMNAR_FIELD = "columnar";
    static final String TASK_ID_FIELD = "task_id";
    static final String HEADERS_FIELD = "headers";
    static final String PAGE_INDEX_FIELD = "page_index";
    static final String DATA_FIELD = "data";

    static final String DOC_TYPE_METADATA = "metadata";
    static final String DOC_TYPE_PAGE = "page";

    private static final int CURSOR_INDEX_MAPPINGS_VERSION = 1;

    private final Client clientWithOrigin;
    private final ClusterService clusterService;
    private final NamedWriteableRegistry registry;
    private final BlockFactory blockFactory;

    public EsqlCursorIndexService(
        Client client,
        ClusterService clusterService,
        NamedWriteableRegistry registry,
        BlockFactory blockFactory
    ) {
        this.clientWithOrigin = new OriginSettingClient(client, ESQL_ORIGIN);
        this.clusterService = clusterService;
        this.registry = registry;
        this.blockFactory = blockFactory;
    }

    public static SystemIndexDescriptor getSystemIndexDescriptor() {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(CURSOR_INDEX + "*")
            .setDescription("ES|QL paginated cursor results")
            .setPrimaryIndex(CURSOR_INDEX)
            .setMappings(mappings())
            .setSettings(settings())
            .setOrigin(ESQL_ORIGIN)
            .build();
    }

    static Settings settings() {
        return Settings.builder()
            .put("index.codec", "best_compression")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .build();
    }

    private static XContentBuilder mappings() {
        try {
            return jsonBuilder().startObject()
                .startObject("_doc")
                .startObject("_meta")
                .field("version", org.elasticsearch.Version.CURRENT)
                .field(SystemIndexDescriptor.VERSION_META_KEY, CURSOR_INDEX_MAPPINGS_VERSION)
                .endObject()
                .field("dynamic", "strict")
                .startObject("properties")
                .startObject(DOC_TYPE_FIELD)
                .field("type", "keyword")
                .endObject()
                .startObject(CURSOR_ID_FIELD)
                .field("type", "keyword")
                .endObject()
                .startObject(EXPIRATION_TIME_FIELD)
                .field("type", "long")
                .endObject()
                .startObject(COLUMNS_FIELD)
                .field("type", "object")
                .field("enabled", "false")
                .endObject()
                .startObject(TOTAL_ROWS_FIELD)
                .field("type", "integer")
                .endObject()
                .startObject(TOTAL_PAGES_FIELD)
                .field("type", "integer")
                .endObject()
                .startObject(ZONE_ID_FIELD)
                .field("type", "keyword")
                .endObject()
                .startObject(COLUMNAR_FIELD)
                .field("type", "boolean")
                .endObject()
                .startObject(TASK_ID_FIELD)
                .field("type", "keyword")
                .endObject()
                .startObject(HEADERS_FIELD)
                .field("type", "object")
                .field("enabled", "false")
                .endObject()
                .startObject(PAGE_INDEX_FIELD)
                .field("type", "integer")
                .endObject()
                .startObject(DATA_FIELD)
                .field("type", "object")
                .field("enabled", "false")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build mappings for " + CURSOR_INDEX, e);
        }
    }

    // ---- Store ----

    /**
     * Stores cursor metadata with the given cursor ID. Used when the caller needs the
     * cursor ID before the index completes (e.g. to send the response immediately).
     */
    public void storeMetadataWithId(
        String cursorId,
        List<ColumnInfoImpl> columns,
        int totalRows,
        int totalPages,
        long expirationMillis,
        ZoneId zoneId,
        boolean columnar,
        @Nullable TaskId taskId,
        Map<String, String> securityHeaders,
        ActionListener<String> listener
    ) {
        try {
            TransportVersion minNodeVersion = clusterService.state().getMinTransportVersion();
            IndexRequest indexRequest = new IndexRequest(CURSOR_INDEX).create(true)
                .id(cursorId)
                .source(
                    buildMetadataSource(
                        cursorId,
                        columns,
                        totalRows,
                        totalPages,
                        expirationMillis,
                        zoneId,
                        columnar,
                        taskId,
                        securityHeaders,
                        minNodeVersion
                    )
                );

            clientWithOrigin.index(
                indexRequest,
                listener.delegateFailureAndWrap((delegate, indexResponse) -> delegate.onResponse(cursorId))
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Stores incomplete metadata (with {@code total_pages = }{@link #TOTAL_PAGES_IN_PROGRESS}) to indicate that the query
     * is still producing pages. Called by {@link StorePageOperator} after the first page is stored.
     *
     * @param taskId the background pagination task ID, stored so the task can be cancelled on cursor delete
     */
    public void storeMetadataIncomplete(
        String cursorId,
        List<ColumnInfoImpl> columns,
        long expirationMillis,
        ZoneId zoneId,
        boolean columnar,
        TaskId taskId,
        Map<String, String> securityHeaders,
        ActionListener<String> listener
    ) {
        storeMetadataWithId(
            cursorId,
            columns,
            TOTAL_PAGES_IN_PROGRESS,
            TOTAL_PAGES_IN_PROGRESS,
            expirationMillis,
            zoneId,
            columnar,
            taskId,
            securityHeaders,
            listener
        );
    }

    /**
     * Stores a single output page document to the cursor index.
     *
     * @param needsDeepCopy when {@code true} the pages are deep-copied before serialization
     *        because they may contain block types (e.g. from {@code SingletonOrdinalsBuilder})
     *        that are not safely serializable. Pages that have already been through
     *        {@link Page#filter} produce standard array-backed blocks and can be stored directly.
     */
    public void storePage(String cursorId, int pageIndex, List<Page> pageData, boolean needsDeepCopy, ActionListener<Void> listener) {
        try {
            List<Page> toStore;
            if (needsDeepCopy) {
                toStore = deepCopyPages(pageData);
            } else {
                toStore = pageData;
            }
            TransportVersion minNodeVersion = clusterService.state().getMinTransportVersion();
            IndexRequest indexRequest = new IndexRequest(CURSOR_INDEX).create(true)
                .id(pageDocId(cursorId, pageIndex))
                .source(buildPageSource(cursorId, pageIndex, toStore, minNodeVersion));
            if (needsDeepCopy) {
                toStore.forEach(Page::releaseBlocks);
            }

            clientWithOrigin.index(indexRequest, listener.map(indexResponse -> null));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Updates the metadata document to mark the cursor as complete with final totals.
     */
    public void updateMetadataComplete(String cursorId, int totalRows, int totalPages, ActionListener<Void> listener) {
        try {
            XContentBuilder source = XContentBuilder.builder(XContentType.JSON.xContent())
                .startObject()
                .field(TOTAL_ROWS_FIELD, totalRows)
                .field(TOTAL_PAGES_FIELD, totalPages)
                .endObject();
            var updateRequest = new org.elasticsearch.action.update.UpdateRequest().index(CURSOR_INDEX)
                .id(cursorId)
                .doc(source)
                .retryOnConflict(5);
            clientWithOrigin.update(updateRequest, listener.map(r -> null));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Marks the metadata document as failed so cursor fetch returns a meaningful error
     * instead of retrying until timeout.
     */
    public void updateMetadataFailed(String cursorId, ActionListener<Void> listener) {
        try {
            XContentBuilder source = XContentBuilder.builder(XContentType.JSON.xContent())
                .startObject()
                .field(TOTAL_PAGES_FIELD, TOTAL_PAGES_FAILED)
                .endObject();
            var updateRequest = new org.elasticsearch.action.update.UpdateRequest().index(CURSOR_INDEX)
                .id(cursorId)
                .doc(source)
                .retryOnConflict(5);
            clientWithOrigin.update(updateRequest, listener.map(r -> null));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private XContentBuilder buildMetadataSource(
        String cursorId,
        List<ColumnInfoImpl> columns,
        int totalRows,
        int totalPages,
        long expirationMillis,
        ZoneId zoneId,
        boolean columnar,
        @Nullable TaskId taskId,
        Map<String, String> securityHeaders,
        TransportVersion minNodeVersion
    ) throws IOException {
        XContentBuilder metaSource = jsonBuilder().startObject()
            .field(DOC_TYPE_FIELD, DOC_TYPE_METADATA)
            .field(CURSOR_ID_FIELD, cursorId)
            .field(EXPIRATION_TIME_FIELD, expirationMillis)
            .field(TOTAL_ROWS_FIELD, totalRows)
            .field(TOTAL_PAGES_FIELD, totalPages)
            .field(ZONE_ID_FIELD, zoneId.getId())
            .field(COLUMNAR_FIELD, columnar);
        if (taskId != null) {
            metaSource.field(TASK_ID_FIELD, taskId.toString());
        }
        if (securityHeaders != null && securityHeaders.isEmpty() == false) {
            metaSource.field(HEADERS_FIELD, securityHeaders);
        }
        metaSource.directFieldAsBase64(COLUMNS_FIELD, os -> {
            os = Streams.noCloseStream(os);
            TransportVersion.writeVersion(minNodeVersion, new OutputStreamStreamOutput(os));
            try (var out = CompressorFactory.COMPRESSOR.threadLocalStreamOutput(os)) {
                out.setTransportVersion(minNodeVersion);
                out.writeCollection(columns);
            }
        });
        metaSource.endObject();
        return metaSource;
    }

    private XContentBuilder buildPageSource(String cursorId, int pageIndex, List<Page> pageData, TransportVersion minNodeVersion)
        throws IOException {
        // pageData is from our deep copy; we release the full copied structure when done storing
        XContentBuilder pageSource = jsonBuilder().startObject()
            .field(DOC_TYPE_FIELD, DOC_TYPE_PAGE)
            .field(CURSOR_ID_FIELD, cursorId)
            .field(PAGE_INDEX_FIELD, pageIndex);
        pageSource.directFieldAsBase64(DATA_FIELD, os -> {
            os = Streams.noCloseStream(os);
            TransportVersion.writeVersion(minNodeVersion, new OutputStreamStreamOutput(os));
            try (var out = CompressorFactory.COMPRESSOR.threadLocalStreamOutput(os)) {
                out.setTransportVersion(minNodeVersion);
                out.writeCollection(pageData);
            }
        });
        pageSource.endObject();
        return pageSource;
    }

    // ---- Get (metadata) ----

    /**
     * Retrieves cursor metadata (columns, totalPages, zoneId, columnar).
     */
    public void getMetadata(String cursorId, ActionListener<CursorMetadata> listener) {
        GetRequest getRequest = new GetRequest(CURSOR_INDEX).id(cursorId).realtime(true);
        clientWithOrigin.get(getRequest, listener.delegateFailure((l, getResponse) -> {
            if (getResponse.isExists() == false) {
                l.onFailure(new ResourceNotFoundException("cursor not found or expired"));
                return;
            }
            try {
                CursorMetadata metadata = parseMetadata(getResponse.getSourceInternal());
                if (metadata.expirationTimeMillis() > 0 && System.currentTimeMillis() >= metadata.expirationTimeMillis()) {
                    l.onFailure(new ResourceNotFoundException("cursor not found or expired"));
                    return;
                }
                l.onResponse(metadata);
            } catch (Exception e) {
                l.onFailure(e);
            }
        }));
    }

    private CursorMetadata parseMetadata(BytesReference source) {
        try (
            XContentParser parser = XContentHelper.createParser(
                org.elasticsearch.xcontent.NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                source,
                XContentType.JSON
            )
        ) {
            ensureExpectedToken(parser.nextToken(), XContentParser.Token.START_OBJECT, parser);
            List<ColumnInfoImpl> columns = null;
            long expirationTime = 0;
            int totalRows = 0;
            int totalPages = 0;
            String zoneIdStr = null;
            boolean columnar = false;
            TaskId taskId = null;
            Map<String, String> headers = Map.of();
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
                parser.nextToken();
                switch (parser.currentName()) {
                    case COLUMNS_FIELD -> columns = decodeColumns(parser.charBuffer());
                    case EXPIRATION_TIME_FIELD -> expirationTime = (long) parser.numberValue();
                    case TOTAL_ROWS_FIELD -> totalRows = parser.intValue();
                    case TOTAL_PAGES_FIELD -> totalPages = parser.intValue();
                    case ZONE_ID_FIELD -> zoneIdStr = parser.text();
                    case COLUMNAR_FIELD -> columnar = parser.booleanValue();
                    case TASK_ID_FIELD -> taskId = new TaskId(parser.text());
                    case HEADERS_FIELD -> {
                        @SuppressWarnings("unchecked")
                        Map<String, String> h = (Map<String, String>) XContentParserUtils.parseFieldsValue(parser);
                        headers = h != null ? h : Map.of();
                    }
                    default -> XContentParserUtils.parseFieldsValue(parser);
                }
            }
            if (columns == null) {
                throw new IllegalStateException("Cursor metadata missing [" + COLUMNS_FIELD + "] field");
            }
            ZoneId zoneId = zoneIdStr != null ? ZoneId.of(zoneIdStr) : ZoneOffset.UTC;
            return new CursorMetadata(columns, totalRows, totalPages, expirationTime, zoneId, columnar, taskId, headers);
        } catch (IOException e) {
            throw new org.elasticsearch.ElasticsearchParseException("Failed to parse cursor metadata", e);
        }
    }

    private List<ColumnInfoImpl> decodeColumns(CharBuffer encodedBuffer) throws IOException {
        InputStream encodedIn = base64InputStream(encodedBuffer);
        TransportVersion version = TransportVersion.readVersion(new InputStreamStreamInput(encodedIn));
        StreamInput input = CompressorFactory.COMPRESSOR.threadLocalStreamInput(encodedIn);
        try (var in = new NamedWriteableAwareStreamInput(input, registry)) {
            in.setTransportVersion(version);
            return in.readCollectionAsList(ColumnInfoImpl::new);
        }
    }

    // ---- Get (page) ----

    /**
     * Retrieves a single page of data by page index.
     */
    public void getPage(String cursorId, int pageIndex, ActionListener<List<Page>> listener) {
        GetRequest getRequest = new GetRequest(CURSOR_INDEX).id(pageDocId(cursorId, pageIndex)).realtime(true);
        clientWithOrigin.get(getRequest, listener.delegateFailure((l, getResponse) -> {
            if (getResponse.isExists() == false) {
                l.onFailure(new ResourceNotFoundException("cursor page not found"));
                return;
            }
            try {
                List<Page> pages = parsePageData(getResponse.getSourceInternal());
                l.onResponse(pages);
            } catch (Exception e) {
                l.onFailure(e);
            }
        }));
    }

    private List<Page> parsePageData(BytesReference source) {
        try (
            XContentParser parser = XContentHelper.createParser(
                org.elasticsearch.xcontent.NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                source,
                XContentType.JSON
            )
        ) {
            ensureExpectedToken(parser.nextToken(), XContentParser.Token.START_OBJECT, parser);
            List<Page> pages = null;
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
                parser.nextToken();
                switch (parser.currentName()) {
                    case DATA_FIELD -> pages = decodePageData(parser.charBuffer());
                    default -> XContentParserUtils.parseFieldsValue(parser);
                }
            }
            if (pages == null) {
                throw new IllegalStateException("Page document missing [" + DATA_FIELD + "] field");
            }
            return pages;
        } catch (IOException e) {
            throw new org.elasticsearch.ElasticsearchParseException("Failed to parse cursor page", e);
        }
    }

    private List<Page> decodePageData(CharBuffer encodedBuffer) throws IOException {
        InputStream encodedIn = base64InputStream(encodedBuffer);
        TransportVersion version = TransportVersion.readVersion(new InputStreamStreamInput(encodedIn));
        StreamInput input = CompressorFactory.COMPRESSOR.threadLocalStreamInput(encodedIn);
        try (BlockStreamInput bsi = new BlockStreamInput(new NamedWriteableAwareStreamInput(input, registry), blockFactory)) {
            bsi.setTransportVersion(version);
            return bsi.readCollectionAsList(Page::new);
        }
    }

    // ---- Delete ----

    /**
     * Deletes all documents for a cursor (metadata + all page docs).
     * Refreshes the index first so that the search phase of delete-by-query can find all documents,
     * including those that were recently written and not yet visible to search.
     */
    public void delete(String cursorId, ActionListener<Boolean> listener) {
        logger.info("delete cursor [{}]: refreshing [{}]", cursorId, CURSOR_INDEX);
        clientWithOrigin.admin().indices().refresh(new RefreshRequest(CURSOR_INDEX), listener.delegateFailureAndWrap((l, refreshResp) -> {
            logger.info("delete cursor [{}]: refresh done, running delete-by-query", cursorId);
            DeleteByQueryRequest dbq = new DeleteByQueryRequest(CURSOR_INDEX).setQuery(QueryBuilders.termQuery(CURSOR_ID_FIELD, cursorId));
            clientWithOrigin.execute(DeleteByQueryAction.INSTANCE, dbq, l.delegateFailureAndWrap((l2, resp) -> {
                logger.info("delete cursor [{}]: delete-by-query done, deleted=[{}]", cursorId, resp.getDeleted());
                l2.onResponse(true);
            }));
        }));
    }

    /**
     * Updates the expiration time of all documents for a cursor.
     */
    public void updateKeepAlive(String cursorId, TimeValue keepAlive, ActionListener<Void> listener) {
        // not critical for correctness — just update the metadata doc; maintenance will handle the rest
        long newExpiration = System.currentTimeMillis() + keepAlive.getMillis();
        var source = java.util.Collections.singletonMap(EXPIRATION_TIME_FIELD, newExpiration);
        var updateRequest = new org.elasticsearch.action.update.UpdateRequest().index(CURSOR_INDEX)
            .id(cursorId)
            .doc(source, XContentType.JSON)
            .retryOnConflict(5);
        clientWithOrigin.update(updateRequest, listener.map(r -> null));
    }

    // ---- Helpers ----

    /**
     * Deep copies pages so that all blocks use serializable backing storage.
     * Some blocks (e.g. from SingletonBytesRefBuilder with constant-length offsets) use
     * lightweight wrappers that can be inconsistent when serialized; copying avoids this.
     */
    private List<Page> deepCopyPages(List<Page> pageData) throws IOException {
        List<Page> result = new ArrayList<>(pageData.size());
        try {
            for (Page page : pageData) {
                Block[] copiedBlocks = new Block[page.getBlockCount()];
                try {
                    for (int b = 0; b < page.getBlockCount(); b++) {
                        copiedBlocks[b] = page.getBlock(b).deepCopy(blockFactory);
                    }
                    result.add(
                        page.batchMetadata() != null
                            ? new Page(page.batchMetadata(), copiedBlocks)
                            : new Page(page.getPositionCount(), copiedBlocks)
                    );
                } catch (Exception e) {
                    Releasables.closeExpectNoException(copiedBlocks);
                    throw e;
                }
            }
            return result;
        } catch (Exception e) {
            result.forEach(Page::releaseBlocks);
            throw e;
        }
    }

    static String pageDocId(String cursorId, int pageIndex) {
        return cursorId + "_p" + pageIndex;
    }

    private static InputStream base64InputStream(CharBuffer encodedBuffer) {
        byte[] decoded = Base64.getDecoder().decode(encodedBuffer.toString());
        return new ByteArrayInputStream(decoded);
    }

    // ---- Data classes ----

    /**
     * Cursor metadata stored in the metadata document.
     */
    public record CursorMetadata(
        List<ColumnInfoImpl> columns,
        int totalRows,
        int totalPages,
        long expirationTimeMillis,
        ZoneId zoneId,
        boolean columnar,
        @Nullable TaskId taskId,
        Map<String, String> headers
    ) {
        /** The query is still running and producing pages. */
        public boolean isInProgress() {
            return totalPages == TOTAL_PAGES_IN_PROGRESS;
        }

        /** The query failed after partial storage. */
        public boolean isFailed() {
            return totalPages == TOTAL_PAGES_FAILED;
        }

        public boolean isComplete() {
            return totalPages >= 0;
        }
    }

}
