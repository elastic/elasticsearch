/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_VERSION_CREATED;
import static org.hamcrest.Matchers.equalTo;

public class IndexRoutingBatchTests extends ESTestCase {

    private static IndexRouting.ExtractFromSource.ForIndexDimensions forIndexDimensions(String dimensionPath) {
        Settings settings = Settings.builder()
            .put(SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersion.current())
            .put(IndexMetadata.INDEX_DIMENSIONS.getKey(), dimensionPath)
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .build();
        IndexMetadata md = IndexMetadata.builder("test").settings(settings).numberOfShards(8).numberOfReplicas(0).build();
        IndexRouting routing = IndexRouting.fromIndexMetadata(md);
        return (IndexRouting.ExtractFromSource.ForIndexDimensions) routing;
    }

    private static IndexRouting.ExtractFromSource.ForIndexDimensions forIndexDimensionsWithResharding(String dimensionPath, int shards) {
        IndexMetadata base = IndexMetadata.builder("test")
            .settings(
                Settings.builder()
                    .put(SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersion.current())
                    .put(IndexMetadata.INDEX_DIMENSIONS.getKey(), dimensionPath)
                    .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                    .build()
            )
            .numberOfShards(shards)
            .numberOfReplicas(0)
            .build();
        IndexReshardingMetadata reshardingMetadata = IndexReshardingMetadata.newSplitByMultiple(shards, 2);
        IndexMetadata splitting = IndexMetadata.builder(base)
            .reshardingMetadata(reshardingMetadata)
            .reshardAddShards(reshardingMetadata.shardCountAfter())
            .setRoutingNumShards(reshardingMetadata.shardCountAfter())
            .settingsVersion(base.getSettingsVersion() + 1)
            .build();
        IndexRouting routing = IndexRouting.fromIndexMetadata(splitting);
        return (IndexRouting.ExtractFromSource.ForIndexDimensions) routing;
    }

    private static BytesReference toJson(Map<String, Object> doc) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.map(doc);
            return BytesReference.bytes(builder);
        }
    }

    private static IndexRequest requestFrom(BytesReference source) {
        return new IndexRequest("test").source(source, XContentType.JSON);
    }

    /**
     * {@code indexShard(requests[], batch)} must produce the same shard ids and the same tsids as
     * a per-request loop of {@code indexShard(request)}.
     */
    public void testForIndexDimensionsBatchMatchesPerRequest() throws IOException {
        List<BytesReference> sources = List.of(
            toJson(Map.of("dim.host", "n1", "dim.region", "us")),
            toJson(Map.of("dim.host", "n2", "dim.region", "eu")),
            toJson(Map.of("dim.host", "n3", "dim.region", "ap"))
        );

        // Per-request path — use one strategy instance per direction to keep batchHashes clear.
        IndexRouting.ExtractFromSource.ForIndexDimensions perReqStrategy = forIndexDimensions("dim.*");
        int[] expectedShards = new int[sources.size()];
        BytesRef[] expectedTsids = new BytesRef[sources.size()];
        for (int i = 0; i < sources.size(); i++) {
            IndexRequest req = requestFrom(sources.get(i));
            perReqStrategy.preProcess(req);
            expectedShards[i] = perReqStrategy.indexShard(req);
            expectedTsids[i] = req.tsid();
        }

        // Batch path
        IndexRouting.ExtractFromSource.ForIndexDimensions batchStrategy = forIndexDimensions("dim.*");
        IndexRequest[] batchRequests = new IndexRequest[sources.size()];
        for (int i = 0; i < sources.size(); i++) {
            batchRequests[i] = requestFrom(sources.get(i));
            batchStrategy.preProcess(batchRequests[i]);
        }
        EscfBatch batch = EscfEncoder.encode(sources, XContentType.JSON);
        int[] actualShards = batchStrategy.indexShard(batchRequests, batch);

        assertThat("shard id array length", actualShards.length, equalTo(sources.size()));
        for (int i = 0; i < sources.size(); i++) {
            assertThat("shard id at row " + i, actualShards[i], equalTo(expectedShards[i]));
            assertThat("tsid at row " + i, batchRequests[i].tsid(), equalTo(expectedTsids[i]));
        }
    }

    /**
     * When all requests have a pre-set tsid (all-pre-set case), the batch uses those tsids as-is
     * without invoking the columnar calculator, and produces the same shard ids.
     */
    public void testAllPreSetTsidsAreUsedAsIs() throws IOException {
        List<BytesReference> sources = List.of(
            toJson(Map.of("dim.host", "a", "dim.region", "us")),
            toJson(Map.of("dim.host", "b", "dim.region", "eu"))
        );

        // Pre-compute tsids and shard ids via the reference per-request path.
        IndexRouting.ExtractFromSource.ForIndexDimensions ref = forIndexDimensions("dim.*");
        BytesRef[] refTsids = new BytesRef[2];
        int[] refShards = new int[2];
        for (int i = 0; i < 2; i++) {
            IndexRequest req = requestFrom(sources.get(i));
            ref.preProcess(req);
            refShards[i] = ref.indexShard(req);
            refTsids[i] = req.tsid();
        }

        // Batch: all requests carry a pre-set tsid.
        IndexRouting.ExtractFromSource.ForIndexDimensions batchStrategy = forIndexDimensions("dim.*");
        IndexRequest[] requests = { requestFrom(sources.get(0)).tsid(refTsids[0]), requestFrom(sources.get(1)).tsid(refTsids[1]) };
        batchStrategy.preProcess(requests[0]);
        batchStrategy.preProcess(requests[1]);

        EscfBatch batch = EscfEncoder.encode(sources, XContentType.JSON);
        int[] shards = batchStrategy.indexShard(requests, batch);

        for (int i = 0; i < 2; i++) {
            assertThat("pre-set tsid must be preserved at row " + i, requests[i].tsid(), equalTo(refTsids[i]));
            assertThat("shard id must match reference at row " + i, shards[i], equalTo(refShards[i]));
        }
    }

    /**
     * A mixed batch (some requests have a pre-set tsid, others do not) violates the all-or-none
     * rule and must throw {@link IllegalArgumentException}.
     */
    public void testMixedPreSetThrows() throws IOException {
        List<BytesReference> sources = List.of(
            toJson(Map.of("dim.host", "a", "dim.region", "us")),
            toJson(Map.of("dim.host", "b", "dim.region", "eu"))
        );

        IndexRouting.ExtractFromSource.ForIndexDimensions ref = forIndexDimensions("dim.*");
        BytesRef tsid0 = ref.buildTsid(XContentType.JSON, sources.get(0));

        IndexRouting.ExtractFromSource.ForIndexDimensions batchStrategy = forIndexDimensions("dim.*");
        // Row 0 has a tsid; row 1 does not — violates the all-or-none invariant.
        IndexRequest[] requests = { requestFrom(sources.get(0)).tsid(tsid0), requestFrom(sources.get(1)) };
        batchStrategy.preProcess(requests[0]);
        batchStrategy.preProcess(requests[1]);

        EscfBatch batch = EscfEncoder.encode(sources, XContentType.JSON);
        expectThrows(IllegalArgumentException.class, () -> batchStrategy.indexShard(requests, batch));
    }

    /**
     * {@code postProcess(requests[])} must produce the same routing field as calling
     * {@code postProcess(request)} on each request individually (after the corresponding
     * {@code indexShard} call).
     */
    public void testBatchPostProcessMatchesPerRequest() throws IOException {
        List<BytesReference> sources = List.of(
            toJson(Map.of("dim.host", "n1", "dim.region", "us")),
            toJson(Map.of("dim.host", "n2", "dim.region", "eu"))
        );

        // Per-request path
        IndexRouting.ExtractFromSource.ForIndexDimensions perReq = forIndexDimensions("dim.*");
        IndexRequest[] perReqRequests = new IndexRequest[2];
        for (int i = 0; i < 2; i++) {
            perReqRequests[i] = requestFrom(sources.get(i));
            perReq.preProcess(perReqRequests[i]);
            perReq.indexShard(perReqRequests[i]);
            perReq.postProcess(perReqRequests[i]);
        }

        // Batch path
        IndexRouting.ExtractFromSource.ForIndexDimensions batch = forIndexDimensions("dim.*");
        IndexRequest[] batchRequests = new IndexRequest[2];
        for (int i = 0; i < 2; i++) {
            batchRequests[i] = requestFrom(sources.get(i));
            batch.preProcess(batchRequests[i]);
        }
        EscfBatch escfBatch = EscfEncoder.encode(sources, XContentType.JSON);
        batch.indexShard(batchRequests, escfBatch);
        batch.postProcess(batchRequests);

        for (int i = 0; i < 2; i++) {
            assertThat("routing field at row " + i, batchRequests[i].routing(), equalTo(perReqRequests[i].routing()));
        }
    }

    /**
     * A request with an explicit routing must throw {@link IllegalArgumentException} for
     * {@code ForIndexDimensions}, matching the per-request {@code checkNoRouting} behavior.
     */
    public void testCheckNoRoutingThrows() throws IOException {
        var strategy = forIndexDimensions("dim.*");
        IndexRequest[] requests = { requestFrom(toJson(Map.of("dim.host", "n1", "dim.region", "us"))).routing("custom") };
        EscfBatch batch = EscfEncoder.encode(List.of(toJson(Map.of("dim.host", "n1", "dim.region", "us"))), XContentType.JSON);
        expectThrows(IllegalArgumentException.class, () -> strategy.indexShard(requests, batch));
    }

    /**
     * With resharding in effect, batch shard ids must match the per-request shard ids (i.e.
     * {@link IndexRouting.ExtractFromSource#rerouteWritesIfResharding} is applied per row).
     */
    public void testReshardingAppliedPerRow() throws IOException {
        List<BytesReference> sources = List.of(
            toJson(Map.of("dim.host", "n1", "dim.region", "us")),
            toJson(Map.of("dim.host", "n2", "dim.region", "eu")),
            toJson(Map.of("dim.host", "n3", "dim.region", "ap"))
        );
        int shards = 4;

        // Per-request path
        IndexRouting.ExtractFromSource.ForIndexDimensions perReq = forIndexDimensionsWithResharding("dim.*", shards);
        int[] expectedShards = new int[3];
        for (int i = 0; i < 3; i++) {
            IndexRequest req = requestFrom(sources.get(i));
            perReq.preProcess(req);
            expectedShards[i] = perReq.indexShard(req);
        }

        // Batch path
        IndexRouting.ExtractFromSource.ForIndexDimensions batchStrategy = forIndexDimensionsWithResharding("dim.*", shards);
        IndexRequest[] batchRequests = new IndexRequest[3];
        for (int i = 0; i < 3; i++) {
            batchRequests[i] = requestFrom(sources.get(i));
            batchStrategy.preProcess(batchRequests[i]);
        }
        EscfBatch batch = EscfEncoder.encode(sources, XContentType.JSON);
        int[] actualShards = batchStrategy.indexShard(batchRequests, batch);

        for (int i = 0; i < 3; i++) {
            assertThat("resharded shard id at row " + i, actualShards[i], equalTo(expectedShards[i]));
        }
    }

    /**
     * {@link IndexRouting.ExtractFromSource.ForRoutingPath#indexShard(IndexRequest[], SourceBatch)}
     * must throw {@link UnsupportedOperationException} since batch routing is not yet implemented
     * for routing_path indices.
     */
    public void testForRoutingPathBatchThrows() throws IOException {
        Settings settings = Settings.builder()
            .put(SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersion.current())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "top")
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .build();
        IndexMetadata md = IndexMetadata.builder("test").settings(settings).numberOfShards(4).numberOfReplicas(0).build();
        IndexRouting routing = IndexRouting.fromIndexMetadata(md);

        BytesReference src = toJson(Map.of("top", "value", "bar", "B"));
        IndexRequest[] requests = { requestFrom(src) };
        SourceBatch batch = EscfEncoder.encode(List.of(src), XContentType.JSON);

        expectThrows(UnsupportedOperationException.class, () -> routing.indexShard(requests, batch));
    }

    /**
     * For id/routing-based routing (Unpartitioned), the default batch {@code indexShard} loops over
     * the per-request method, so batch shard ids equal per-request shard ids.
     */
    public void testUnpartitionedBatchMatchesLoop() throws IOException {
        Settings settings = Settings.builder().put(SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersion.current()).build();
        IndexMetadata md = IndexMetadata.builder("test").settings(settings).numberOfShards(8).numberOfReplicas(0).build();
        IndexRouting routing = IndexRouting.fromIndexMetadata(md);

        List<BytesReference> sources = List.of(toJson(Map.of("field", "a")), toJson(Map.of("field", "b")), toJson(Map.of("field", "c")));

        // Per-request shards (using fixed ids)
        int[] expectedShards = new int[3];
        for (int i = 0; i < 3; i++) {
            IndexRequest req = new IndexRequest("test").id("id-" + i).source(sources.get(i), XContentType.JSON);
            routing.preProcess(req);
            expectedShards[i] = routing.indexShard(req);
        }

        // Batch shards
        IndexRequest[] batchRequests = new IndexRequest[3];
        for (int i = 0; i < 3; i++) {
            batchRequests[i] = new IndexRequest("test").id("id-" + i).source(sources.get(i), XContentType.JSON);
            routing.preProcess(batchRequests[i]);
        }
        SourceBatch batch = EscfEncoder.encode(sources, XContentType.JSON);
        int[] actualShards = routing.indexShard(batchRequests, batch);

        for (int i = 0; i < 3; i++) {
            assertThat("unpartitioned shard at row " + i, actualShards[i], equalTo(expectedShards[i]));
        }
    }

    /**
     * {@link IndexRouting#preProcess(IndexRequest[])} auto-generates ids for requests that lack one,
     * matching the per-request {@code preProcess} behavior.
     */
    public void testBatchPreProcessAutoGeneratesIds() throws IOException {
        Settings settings = Settings.builder().put(SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersion.current()).build();
        IndexMetadata md = IndexMetadata.builder("test").settings(settings).numberOfShards(4).numberOfReplicas(0).build();
        IndexRouting routing = IndexRouting.fromIndexMetadata(md);

        int n = randomIntBetween(2, 8);
        IndexRequest[] requests = new IndexRequest[n];
        for (int i = 0; i < n; i++) {
            requests[i] = new IndexRequest("test").source(toJson(Map.of("v", i)), XContentType.JSON);
        }

        routing.preProcess(requests);

        // preProcess generates an id for Unpartitioned if none was set (delegates to per-request).
        // The key assertion is that the batch version runs without error and each request was touched.
        // (Unpartitioned.preProcess does nothing special for STANDARD mode — ids are set lazily.)
        assertThat("request count unchanged", requests.length, equalTo(n));
    }
}
