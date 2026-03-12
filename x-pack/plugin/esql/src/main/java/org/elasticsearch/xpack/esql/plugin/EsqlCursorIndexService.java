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
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
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
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Streams;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.CharBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

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
    static final String ZONE_ID_FIELD = "zone_id";
    static final String COLUMNAR_FIELD = "columnar";
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
     * Stores cursor metadata only. The first page is returned directly to the client and does
     * not need to be persisted. Remaining pages should be stored via {@link #storeRemainingPages}.
     *
     * @param listener called with the cursor ID on success
     */
    public void storeMetadata(
        List<ColumnInfoImpl> columns,
        int totalRows,
        int totalPages,
        long expirationMillis,
        ZoneId zoneId,
        boolean columnar,
        ActionListener<String> listener
    ) {
        String cursorId = UUIDs.randomBase64UUID();
        try {
            TransportVersion minNodeVersion = clusterService.state().getMinTransportVersion();
            IndexRequest indexRequest = new IndexRequest(CURSOR_INDEX).create(true)
                .id(cursorId)
                .source(buildMetadataSource(columns, totalRows, totalPages, expirationMillis, zoneId, columnar, minNodeVersion));

            clientWithOrigin.index(
                indexRequest,
                listener.delegateFailureAndWrap((delegate, indexResponse) -> { delegate.onResponse(cursorId); })
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    static final int PAGES_PER_BULK = 10;

    /**
     * Stores output pages (from {@code fromPageIndex} onwards) asynchronously in batches of
     * {@link #PAGES_PER_BULK}. Each batch is sent as a separate bulk request; the next batch
     * is submitted once the previous one completes, so earlier pages become available sooner.
     * Failures are logged but not propagated to the caller since the first page has already
     * been returned to the client.
     * <p>
     * Deep copies all pages synchronously before returning so the caller may release the
     * originals immediately. The copies are released when the last batch is stored or on error.
     */
    public void storeRemainingPages(String cursorId, List<List<Page>> outputPages, int fromPageIndex, long expirationMillis) {
        if (fromPageIndex >= outputPages.size()) {
            return;
        }
        try {
            List<List<Page>> copiedPages = deepCopyAllPages(outputPages);
            TransportVersion minNodeVersion = clusterService.state().getMinTransportVersion();
            storeBatch(cursorId, copiedPages, fromPageIndex, expirationMillis, minNodeVersion);
        } catch (Exception e) {
            logger.warn("failed to build cursor pages for [{}]", cursorId, e);
        }
    }

    /**
     * Deep copies the entire output structure so we can store asynchronously without
     * depending on the caller's pages (which may be released before later batches run).
     */
    private List<List<Page>> deepCopyAllPages(List<List<Page>> outputPages) throws IOException {
        List<List<Page>> result = new ArrayList<>(outputPages.size());
        try {
            for (List<Page> pageList : outputPages) {
                result.add(deepCopyPages(pageList));
            }
            return result;
        } catch (Exception e) {
            result.forEach(list -> list.forEach(Page::releaseBlocks));
            throw e;
        }
    }

    private void storeBatch(
        String cursorId,
        List<List<Page>> outputPages,
        int fromPageIndex,
        long expirationMillis,
        TransportVersion minNodeVersion
    ) {
        int endIndex = Math.min(fromPageIndex + PAGES_PER_BULK, outputPages.size());
        try {
            BulkRequest bulkRequest = new BulkRequest();
            for (int i = fromPageIndex; i < endIndex; i++) {
                bulkRequest.add(
                    new IndexRequest(CURSOR_INDEX).create(true)
                        .id(pageDocId(cursorId, i))
                        .source(buildPageSource(cursorId, i, outputPages.get(i), expirationMillis, minNodeVersion))
                );
            }

            clientWithOrigin.bulk(bulkRequest, ActionListener.wrap(bulkResponse -> {
                if (bulkResponse.hasFailures()) {
                    logger.warn("failed to store cursor pages for [{}]: {}", cursorId, bulkResponse.buildFailureMessage());
                }
                if (endIndex < outputPages.size()) {
                    storeBatch(cursorId, outputPages, endIndex, expirationMillis, minNodeVersion);
                } else {
                    outputPages.forEach(list -> list.forEach(Page::releaseBlocks));
                }
            }, e -> {
                outputPages.forEach(list -> list.forEach(Page::releaseBlocks));
                logger.warn("failed to store cursor pages for [{}]", cursorId, e);
            }));
        } catch (Exception e) {
            outputPages.forEach(list -> list.forEach(Page::releaseBlocks));
            logger.warn("failed to build cursor pages for [{}]", cursorId, e);
        }
    }

    private XContentBuilder buildMetadataSource(
        List<ColumnInfoImpl> columns,
        int totalRows,
        int totalPages,
        long expirationMillis,
        ZoneId zoneId,
        boolean columnar,
        TransportVersion minNodeVersion
    ) throws IOException {
        XContentBuilder metaSource = jsonBuilder().startObject()
            .field(DOC_TYPE_FIELD, DOC_TYPE_METADATA)
            .field(EXPIRATION_TIME_FIELD, expirationMillis)
            .field(TOTAL_ROWS_FIELD, totalRows)
            .field(TOTAL_PAGES_FIELD, totalPages)
            .field(ZONE_ID_FIELD, zoneId.getId())
            .field(COLUMNAR_FIELD, columnar);
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

    private XContentBuilder buildPageSource(
        String cursorId,
        int pageIndex,
        List<Page> pageData,
        long expirationMillis,
        TransportVersion minNodeVersion
    ) throws IOException {
        // pageData is from our deep copy; we release the full copied structure when done storing
        XContentBuilder pageSource = jsonBuilder().startObject()
            .field(DOC_TYPE_FIELD, DOC_TYPE_PAGE)
            .field(CURSOR_ID_FIELD, cursorId)
            .field(EXPIRATION_TIME_FIELD, expirationMillis)
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
                    default -> XContentParserUtils.parseFieldsValue(parser);
                }
            }
            if (columns == null) {
                throw new IllegalStateException("Cursor metadata missing [" + COLUMNS_FIELD + "] field");
            }
            return new CursorMetadata(columns, totalRows, totalPages, expirationTime, ZoneId.of(zoneIdStr), columnar);
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
     */
    public void delete(String cursorId, ActionListener<Boolean> listener) {
        CountDownActionListener countDown = new CountDownActionListener(2, listener.map(v -> true));

        // delete all page docs by cursor_id
        DeleteByQueryRequest dbq = new DeleteByQueryRequest(CURSOR_INDEX).setQuery(QueryBuilders.termQuery(CURSOR_ID_FIELD, cursorId));
        clientWithOrigin.execute(DeleteByQueryAction.INSTANCE, dbq, countDown.map(r -> null));

        // delete metadata doc
        clientWithOrigin.delete(new DeleteRequest(CURSOR_INDEX).id(cursorId), countDown.map(r -> null));
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
        return Base64.getDecoder().wrap(new InputStream() {
            @Override
            public int read() {
                if (encodedBuffer.hasRemaining()) {
                    return encodedBuffer.get();
                } else {
                    return -1;
                }
            }
        });
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
        boolean columnar
    ) {}

    /**
     * Holds a fetched page along with its cursor metadata.
     */
    public record CursorPage(CursorMetadata metadata, List<Page> pages) {
        public void releasePages() {
            Releasables.close(() -> pages.forEach(p -> Releasables.closeExpectNoException(p::releaseBlocks)));
        }
    }
}
