/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockFactoryProvider;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.fetch.FetchRequest;
import org.elasticsearch.xpack.esql.fetch.FetchResponse;
import org.elasticsearch.xpack.esql.fetch.FetchService;
import org.elasticsearch.xpack.esql.fetch.NodeFetchContexts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * The fetch action between two nodes. A node that didn't run the query phase asks the node that holds the documents for
 * their columns, over the wire. The data node finds the reader contexts the query phase kept open, loads the documents
 * in one driver, and returns their rows in request order. The contexts stay open until the coordinator frees them.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 2)
public class FetchActionIT extends AbstractEsqlIntegTestCase {
    private static final String INDEX = "fetched";
    private static final int DOCS = 50;
    private static final Attribute ID = field("id", DataType.KEYWORD);
    private static final Attribute N = field("n", DataType.LONG);

    /**
     * The open context of one shard and where each document of the shard is, by its {@code n}.
     */
    private record OpenShard(ShardId shardId, ShardSearchContextId contextId, Map<Long, int[]> segmentAndDocByN) {}

    /**
     * The request and the response cross the wire.
     */
    public void testFetchesFromAnotherNode() throws Exception {
        String dataNode = randomDataNode();
        fetchAndCheck(dataNode, randomValueOtherThan(dataNode, () -> randomFrom(internalCluster().getNodeNames())));
    }

    /**
     * When the coordinator holds the documents itself, the request and the response are handed over without
     * serialization, through the same handler and the same authorization.
     */
    public void testFetchesOnTheSameNode() throws Exception {
        String dataNode = randomDataNode();
        fetchAndCheck(dataNode, dataNode);
    }

    private void fetchAndCheck(String dataNode, String coordinator) throws Exception {
        int shards = between(1, 4);
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(indexSettings(shards, 0).put("index.routing.allocation.require._name", dataNode))
                .setMapping("id", "type=keyword", "n", "type=long")
        );
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < DOCS; i++) {
            bulk.add(prepareIndex(INDEX).setSource("id", "doc-" + i, "n", i));
        }
        assertFalse(bulk.get().hasFailures());
        ensureGreen(INDEX);

        List<OpenShard> open = openEveryShard(dataNode, shards);
        List<FetchRequest.ShardDocs> shardDocs = new ArrayList<>();
        List<Tuple<String, Long>> expected = new ArrayList<>();
        for (OpenShard shard : shuffledList(open)) {
            List<Map.Entry<Long, int[]>> wanted = new ArrayList<>(randomSubsetOf(shard.segmentAndDocByN().entrySet()));
            wanted.sort((a, b) -> {
                int segments = Integer.compare(a.getValue()[0], b.getValue()[0]);
                return segments != 0 ? segments : Integer.compare(a.getValue()[1], b.getValue()[1]);
            });
            int[] segments = wanted.stream().mapToInt(e -> e.getValue()[0]).toArray();
            int[] docs = wanted.stream().mapToInt(e -> e.getValue()[1]).toArray();
            shardDocs.add(new FetchRequest.ShardDocs(shard.shardId(), shard.contextId(), false, segments, docs));
            for (Map.Entry<Long, int[]> doc : wanted) {
                expected.add(Tuple.tuple("doc-" + doc.getKey(), doc.getKey()));
            }
        }

        assertThat(fetch(dataNode, coordinator, shardDocs).actionGet(TimeValue.timeValueSeconds(30)), equalTo(expected));

        // a shard whose context is gone fails the whole request, like a fetch request of _search
        OpenShard any = randomFrom(open);
        ShardSearchContextId gone = new ShardSearchContextId(any.contextId().getSessionId(), Long.MAX_VALUE);
        shardDocs.add(
            between(0, shardDocs.size()),
            new FetchRequest.ShardDocs(any.shardId(), gone, false, new int[] { 0 }, new int[] { 0 })
        );
        Exception e = expectThrows(
            Exception.class,
            () -> fetch(dataNode, coordinator, shardDocs).actionGet(TimeValue.timeValueSeconds(30))
        );
        assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(SearchContextMissingException.class));

        // a fetch frees no context it doesn't read for the last time, the coordinator does once the query ends
        SearchService searchService = internalCluster().getInstance(SearchService.class, dataNode);
        assertThat(searchService.getActiveContexts(), equalTo(open.size()));
        for (OpenShard shard : open) {
            assertTrue(searchService.freeReaderContext(shard.contextId()));
        }
        assertBusy(() -> assertThat(searchService.getActiveContexts(), equalTo(0)));
        ensureBlocksReleased();
    }

    /**
     * Sends a fetch request for {@code shardDocs} from {@code coordinator} to {@code dataNode}, and reads the rows of the
     * response.
     */
    private PlainActionFuture<List<Tuple<String, Long>>> fetch(
        String dataNode,
        String coordinator,
        List<FetchRequest.ShardDocs> shardDocs
    ) {
        FetchRequest request = new FetchRequest(
            "session",
            "",
            new OriginalIndices(new String[] { INDEX }, IndicesOptions.strictExpandOpen()),
            shardDocs,
            EsqlTestUtils.TEST_CFG,
            List.of(ID, N)
        );
        request.setParentTask(new TaskId(clusterService(coordinator).localNode().getId(), randomNonNegativeLong()));
        PlainActionFuture<List<Tuple<String, Long>>> future = new PlainActionFuture<>();
        BlockFactory blockFactory = internalCluster().getInstance(BlockFactoryProvider.class, coordinator).blockFactory();
        TransportService transportService = internalCluster().getInstance(TransportService.class, coordinator);
        transportService.sendRequest(
            clusterService(dataNode).localNode(),
            FetchService.FETCH_ACTION_NAME,
            request,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(future.map(response -> {
                List<Page> pages = response.takePages();
                try {
                    return rows(pages);
                } finally {
                    Releasables.closeExpectNoException(pages);
                }
            }), in -> {
                try (BlockStreamInput blocks = new BlockStreamInput(in, blockFactory)) {
                    return new FetchResponse(blocks, transportService.getThreadPool().getThreadContext());
                }
            }, EsExecutors.DIRECT_EXECUTOR_SERVICE)
        );
        return future;
    }

    private static String randomDataNode() {
        return randomFrom(
            Arrays.stream(internalCluster().getNodeNames()).filter(node -> clusterService(node).localNode().canContainData()).toList()
        );
    }

    /**
     * Opens a context on every shard of the index on {@code node}, the way a data node request of the query phase does,
     * finds where each document is, and responds, so the contexts stay open for the fetch.
     */
    private List<OpenShard> openEveryShard(String node, int shards) throws IOException {
        SearchService searchService = internalCluster().getInstance(SearchService.class, node);
        NodeFetchContexts contexts = new NodeFetchContexts(
            searchService,
            null,
            TimeValue.timeValueMillis(searchService.getDefaultKeepAliveInMillis())
        );
        List<SearchContext> opened = new ArrayList<>(shards);
        List<OpenShard> open = new ArrayList<>(shards);
        try {
            for (int shard = 0; shard < shards; shard++) {
                ShardId shardId = new ShardId(resolveIndex(INDEX), shard);
                SearchContext searchContext = contexts.open(
                    new ShardSearchRequest(shardId, System.currentTimeMillis(), AliasFilter.EMPTY, null, SplitShardCountSummary.UNSET)
                );
                opened.add(searchContext);
                contexts.originOf(searchContext);
                open.add(new OpenShard(shardId, searchContext.readerContext().id(), locate(searchContext)));
            }
        } finally {
            opened.forEach(SearchContext::close);
            contexts.close();
        }
        return open;
    }

    private static Map<Long, int[]> locate(SearchContext searchContext) throws IOException {
        Map<Long, int[]> locations = new HashMap<>();
        List<LeafReaderContext> leaves = searchContext.searcher().getIndexReader().leaves();
        for (int segment = 0; segment < leaves.size(); segment++) {
            LeafReader reader = leaves.get(segment).reader();
            SortedNumericDocValues values = DocValues.getSortedNumeric(reader, "n");
            for (int doc = 0; doc < reader.maxDoc(); doc++) {
                assertTrue(values.advanceExact(doc));
                locations.put(values.nextValue(), new int[] { segment, doc });
            }
        }
        return locations;
    }

    private static List<Tuple<String, Long>> rows(List<Page> pages) {
        List<Tuple<String, Long>> rows = new ArrayList<>();
        BytesRef scratch = new BytesRef();
        for (Page page : pages) {
            BytesRefBlock ids = page.getBlock(0);
            LongBlock ns = page.getBlock(1);
            for (int p = 0; p < page.getPositionCount(); p++) {
                String id = ids.getBytesRef(ids.getFirstValueIndex(p), scratch).utf8ToString();
                rows.add(Tuple.tuple(id, ns.getLong(ns.getFirstValueIndex(p))));
            }
        }
        return rows;
    }

    private static Attribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }

    private static ClusterService clusterService(String node) {
        return internalCluster().getInstance(ClusterService.class, node);
    }
}
