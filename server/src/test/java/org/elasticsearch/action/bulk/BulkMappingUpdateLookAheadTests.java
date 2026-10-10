/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Checks that sending the mapping updates of several operations of a bulk request together is not observable: a request must
 * leave the same mappings, the same documents and the same response for every operation as when each of its operations is sent
 * in a request of its own, which never combines mapping updates.
 */
public class BulkMappingUpdateLookAheadTests extends IndexShardTestCase {

    private static final String MAPPING = """
        {
          "_doc": {
            "dynamic_templates": [
              { "ids": { "match": "id_*", "mapping": { "type": "keyword" } } },
              { "ips": { "match": "ip_*", "match_mapping_type": "string", "mapping": { "type": "ip" } } },
              { "short_keyword": { "match": "unmatched", "mapping": { "type": "keyword", "ignore_above": 4 } } },
              { "long_keyword": { "match": "unmatched", "mapping": { "type": "keyword", "ignore_above": 1024 } } },
              { "counter": { "match": "unmatched", "mapping": { "type": "long", "meta": { "kind": "counter" } } } },
              { "runtime_long": { "match": "unmatched", "runtime": { "type": "long" } } }
            ],
            "properties": {
              "strict": { "type": "object", "dynamic": "strict" },
              "static": { "type": "object", "dynamic": "false" },
              "runtime": { "type": "object", "dynamic": "runtime" },
              "flat": { "type": "object", "subobjects": false }
            }
          }
        }""";

    public void testSameOutcomeAsOneOperationPerRequest() throws Exception {
        Settings.Builder settings = Settings.builder();
        if (randomBoolean()) {
            settings.put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), between(6, 25));
        }
        settings.put(MapperService.INDEX_MAPPING_IGNORE_DYNAMIC_BEYOND_LIMIT_SETTING.getKey(), randomBoolean());
        IndexShard separate = newShardWithMapping(settings.build());
        settings.put(MapperService.INDEX_MAPPING_COMBINE_DYNAMIC_UPDATES_SETTING.getKey(), true);
        IndexShard combined = newShardWithMapping(settings.build());
        try {
            int rounds = between(1, 3);
            for (int round = 0; round < rounds; round++) {
                List<Supplier<DocWriteRequest<?>>> operations = randomOperations(round);

                BulkItemRequest[] combinedItems = new BulkItemRequest[operations.size()];
                BulkItemRequest[] separateItems = new BulkItemRequest[operations.size()];
                for (int i = 0; i < combinedItems.length; i++) {
                    boolean aborted = rarely();
                    combinedItems[i] = newItem(i, operations.get(i).get(), aborted);
                    separateItems[i] = newItem(i, operations.get(i).get(), aborted);
                }
                // both shards read the documents from the rows of the same batch, or from their bytes
                boolean batched = randomBoolean();
                int combinedUpdates;
                int separateUpdates = 0;
                try (
                    EscfBatch combinedBatch = batched ? encodeAsBatch(combinedItems) : null;
                    EscfBatch separateBatch = combinedBatch != null ? encodeAsBatch(separateItems) : null
                ) {
                    combinedUpdates = performOnPrimary(combined, combinedItems, combinedBatch);
                    for (BulkItemRequest item : separateItems) {
                        separateUpdates += performOnPrimary(separate, new BulkItemRequest[] { item }, separateBatch);
                    }
                }
                for (int i = 0; i < operations.size(); i++) {
                    BulkItemResponse expected = separateItems[i].getPrimaryResponse();
                    BulkItemResponse actual = combinedItems[i].getPrimaryResponse();
                    String operation = "operation [" + i + "] " + operations.get(i).get();
                    assertThat(operation, actual.isFailed(), equalTo(expected.isFailed()));
                    assertThat(operation, actual.getFailureMessage(), equalTo(expected.getFailureMessage()));
                    if (expected.isFailed() == false) {
                        assertThat(operation, actual.getResponse().getResult(), equalTo(expected.getResponse().getResult()));
                    }
                }
                assertThat(
                    combined.mapperService().documentMapper().mappingSource(),
                    equalTo(separate.mapperService().documentMapper().mappingSource())
                );
                assertThat(combinedUpdates, lessThanOrEqualTo(separateUpdates));
            }
            assertThat(getDocIdAndSeqNos(combined), equalTo(getDocIdAndSeqNos(separate)));
        } finally {
            closeShards(combined, separate);
        }
    }

    private IndexShard newShardWithMapping(Settings settings) throws Exception {
        IndexShard shard = newStartedShard(true, settings);
        shard.mapperService()
            .merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(MAPPING), MapperService.MergeReason.MAPPING_UPDATE);
        return shard;
    }

    private static BulkItemRequest newItem(int id, DocWriteRequest<?> operation, boolean aborted) {
        BulkItemRequest item = new BulkItemRequest(id, operation);
        if (aborted) {
            item.abort("index", new ElasticsearchException("aborted"));
        }
        return item;
    }

    /**
     * Makes the index requests read their document from a row of a batch, the form that bulk requests take on the nodes that
     * batch them. Returns null if the documents can't be encoded.
     */
    private static EscfBatch encodeAsBatch(BulkItemRequest[] items) {
        List<IndexRequest> indexRequests = new ArrayList<>();
        List<BytesReference> sources = new ArrayList<>();
        for (BulkItemRequest item : items) {
            if (item.request() instanceof IndexRequest indexRequest) {
                if (indexRequest.source().utf8ToString().contains("[")) {
                    // rows that hold arrays trip an assertion of the document parser
                    return null;
                }
                indexRequests.add(indexRequest);
                sources.add(indexRequest.source());
            }
        }
        if (sources.isEmpty()) {
            return null;
        }
        final EscfBatch batch;
        try {
            batch = EscfEncoder.encode(sources, XContentType.JSON);
        } catch (Exception e) {
            return null;
        }
        for (int i = 0; i < indexRequests.size(); i++) {
            indexRequests.get(i).indexSource().setSourceRow(batch, i, XContentType.JSON);
        }
        return batch;
    }

    /**
     * Executes the request on the primary, applying the requested mapping updates directly to the mappings of the shard.
     *
     * @return the number of mapping updates that were requested
     */
    private int performOnPrimary(IndexShard shard, BulkItemRequest[] items, @Nullable EscfBatch batch) throws Exception {
        BulkShardRequest request = new BulkShardRequest(shard.shardId(), SplitShardCountSummary.IRRELEVANT, RefreshPolicy.NONE, items);
        if (batch != null) {
            request.setBulkShardBatch(new BulkShardBatch(batch));
        }
        List<CompressedXContent> updates = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        TransportShardBulkAction.performOnPrimary(request, shard, null, threadPool::absoluteTimeInMillis, (update, shardId, listener) -> {
            updates.add(update);
            ActionListener.completeWith(listener, () -> {
                shard.mapperService().merge(MapperService.SINGLE_MAPPING_NAME, update, MapperService.MergeReason.MAPPING_AUTO_UPDATE);
                return null;
            });
        },
            (listener, mappingVersion) -> listener.onResponse(null),
            new LatchedActionListener<>(ActionTestUtils.assertNoFailureListener(result -> {}), latch),
            threadPool.executor(Names.WRITE)
        );
        latch.await();
        return updates.size();
    }

    private List<Supplier<DocWriteRequest<?>>> randomOperations(int round) {
        int numOperations = between(1, 40);
        // the smaller the pool of fields, the more often documents map the same field, in the same way or not
        int numFields = between(1, 20);
        List<Supplier<DocWriteRequest<?>>> operations = new ArrayList<>(numOperations);
        for (int i = 0; i < numOperations; i++) {
            String id = round + "_" + (rarely() ? between(0, i) : i);
            if (rarely()) {
                operations.add(() -> new DeleteRequest("index", id));
                continue;
            }
            String source = rarely() ? "{\"field\": not json" : randomSource(numFields);
            boolean create = randomBoolean();
            // the dynamic templates of a request can map the same field differently in two documents
            Map<String, String> dynamicTemplates = randomBoolean()
                ? Map.of()
                : Map.of("field_" + between(0, numFields - 1), randomFrom("short_keyword", "long_keyword", "counter", "runtime_long"));
            operations.add(
                () -> new IndexRequest("index").id(id)
                    .source(source, XContentType.JSON)
                    .create(create)
                    .setDynamicTemplates(dynamicTemplates)
            );
        }
        return operations;
    }

    private String randomSource(int numFields) {
        StringBuilder source = new StringBuilder("{");
        int numEntries = between(0, 4);
        for (int i = 0; i < numEntries; i++) {
            if (i > 0) {
                source.append(',');
            }
            int field = between(0, numFields - 1);
            switch (between(0, 9)) {
                case 0 -> source.append("\"strict\":{\"field_").append(field).append("\":1}");
                case 1 -> source.append("\"static\":{\"field_").append(field).append("\":1}");
                case 2 -> source.append("\"runtime\":{\"field_").append(field).append("\":").append(randomValue()).append('}');
                case 3 -> source.append("\"flat\":{\"field.").append(field).append("\":").append(randomValue()).append('}');
                case 4 -> source.append("\"id_").append(field).append("\":").append(randomValue());
                case 5 -> source.append("\"ip_").append(field).append("\":").append(randomFrom("\"10.0.0.1\"", "\"not an ip\"", "1"));
                default -> source.append("\"field_").append(field).append("\":").append(randomValue());
            }
        }
        return source.append('}').toString();
    }

    private String randomValue() {
        return switch (between(0, 9)) {
            case 0 -> "1.5";
            case 1 -> "\"some text\"";
            case 2 -> "true";
            case 3 -> "\"2024-01-01\"";
            case 4 -> "{\"sub_" + between(0, 2) + "\":" + randomFrom("1", "\"text\"") + "}";
            case 5 -> "[1,2]";
            case 6 -> "null";
            case 7 -> "[{\"sub_0\":1},{\"sub_1\":true}]";
            default -> Integer.toString(between(0, 100));
        };
    }
}
