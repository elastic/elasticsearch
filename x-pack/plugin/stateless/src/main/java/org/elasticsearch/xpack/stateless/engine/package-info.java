/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

/**
 * <h2>Stateless real-time GET</h2>
 *
 * Real-time get (RTG) requests for non-fast-refresh indices (see {@link}org.elasticsearch.index.IndexSettings#INDEX_FAST_REFRESH_SETTING)
 * are always routed to the search nodes. The search node then sends a
 * {@link org.elasticsearch.action.get.TransportGetFromTranslogAction.Request} request to the indexing node.
 * If the requested doc has a recent version on the indexing node's Translog, it sends it to the search node as a reply to the
 * GetFromTranslog request, and the search node uses this to handle the RTG request. If not, the search node handles the request locally.
 * Note that the reply to the {@link org.elasticsearch.action.get.TransportGetFromTranslogAction} might contain a segment generation
 * (see {@link org.elasticsearch.action.get.TransportGetFromTranslogAction.Response}#segmentGeneration)
 * that the search node must wait for, before handling the GET request locally. The generation returned is taken from
 * {@link org.elasticsearch.index.engine.InternalEngine#getLastUnsafeSegmentGenerationForGets()} which is the last known generation
 * during which the LiveVersionMap was in unsafe mode. When in unsafe mode, the LiveVersionMap on the indexing node does not record
 * any docs, and therefore, the indexing node is not able to handle any GetFromTranslog requests correctly. To address this, the indexing
 * node forces a flush and records the segment generation (see {@link org.elasticsearch.index.engine.InternalEngine}#getVersionFromMap),
 * and returns the segment generation that the search node should wait for, before handling the GET request locally.
 * <p>
 *
 * Indexing node           Search node            Client
 * ─────┬───────           ──────┬─────           ────┬────
 *      │                        │                    │
 *      │                        │      RTG request   │
 *      │    GetFromTranslog     │◄───────────────────┤
 *      │◄───────────────────────┤                    │
 *      │                        │                    │
 *      ├───────────────────────►│                    │
 *      │    GetResult or        │                    │
 *      │ Generation to wait for ├───────────────────►│
 *      │                        │      GetResult     │
 *      │                        │                    │
 *      │                        │                    │
 *
 * <h3>LiveVersionMap</h3>
 *
 * The lifecycle of the entries kept in the LVM are controlled by refreshes. While all refreshes in Stateful ES are local to the node,
 * Stateless ES refreshes are a distributed operation involving both the indexing and the search nodes. Some refreshes performed on the
 * indexing node are converted to a flush to commit the data
 * (see {@link co.elastic.elasticsearch.stateless.engine.IndexEngine#refreshInternalSearcher} and
 * {@link co.elastic.elasticsearch.stateless.engine.IndexEngine}#doExternalRefresh). Upon commit, in order to refresh the unpromotable
 * shards, the indexing node sends a notification ({@link co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction})
 * to the search nodes to notify them of the new commit that is available
 * (see {@link org.elasticsearch.action.admin.indices.refresh.TransportUnpromotableShardRefreshAction}).
 *
 * In ES when we index or delete a doc we record the operation in the {@link org.elasticsearch.index.engine.LiveVersionMap}. This is
 * recorded in the `current` map (step 1). During a local refresh on the indexing shard, we move the doc IDs from the `current` map
 * to the `old` map (step 2), and from the `old` map to the archive (step 3).
 * <p>
 *
 *    LiveVersionMap (LVM)
 *   ┌──────────────────────────────────────────┐
 *   │   Maps                     Archive       │
 *   │  ┌────────────────────┐   ┌───────────┐  │
 *   │  │                    │   │ gen->old  │  │
 *   │  │ ┌────────┐ ┌─────┐ │   │     .     │  │
 *   │  │ │ current│ │ old │ │   │     .     │  │
 *   │  │ └─▲─┬────┘ └─▲─┬─┘ │   │     .     │  │
 * ──┼──┼───┘ └────────┘ └───┼──►│           │◄─┼───────
 *   │  │ 1       2        3 │   │           │  │ 4
 *   │  └────────────────────┘   └───────────┘  │
 *   └──────────────────────────────────────────┘
 *
 * <p>
 * The LiveVersionMap is used to get docs by ID while a refresh has not happened yet, or it is in progress. Once a local refresh is
 * finished, the entries from the old map can be moved out. However, in stateless ES, a local refresh is not enough to make the data
 * searchable. Since GETs (and mGETs) run on the search nodes, we need to be able to perform the lookup on the LVM until the search
 * shards have received the new commit that contains the entries that were moved from the current to the old map. The LVM archive keeps
 * track of the entries that have been moved out of the old map due to a local refresh, but are not yet available on the search nodes,
 * i.e., the unpromotable refresh is in progress. The archive is used when doing a lookup on the LVM
 * (see {@link org.elasticsearch.index.engine.LiveVersionMap}#getUnderLock). In Stateful, this is a no-op since we use a
 * {@link org.elasticsearch.index.engine.LiveVersionMapArchive}#NOOP_ARCHIVE.
 * <p>
 * The StatelessLiveVersionMapArchive is notified when the unpromotable replicas are refreshed (step 4), and this leads to pruning
 * the entries kept in the archive
 * (see {@link co.elastic.elasticsearch.stateless.engine.StatelessLiveVersionMapArchive#afterUnpromotablesRefreshed}).
 *
 */
package co.elastic.elasticsearch.stateless.engine;
