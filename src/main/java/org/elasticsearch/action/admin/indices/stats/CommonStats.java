/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.warmer.WarmerStats;

import java.io.IOException;

/**
 */
public class CommonStats implements Streamable, ToXContent {

    @Nullable
    DocsStats docs;

    @Nullable
    StoreStats store;

    @Nullable
    IndexingStats indexing;

    @Nullable
    GetStats get;

    @Nullable
    SearchStats search;

    @Nullable
    MergeStats merge;

    @Nullable
    RefreshStats refresh;

    @Nullable
    FlushStats flush;

    @Nullable
    WarmerStats warmer;

    public void add(CommonStats stats) {
        if (docs == null) {
            if (stats.docs() != null) {
                docs = new DocsStats();
                docs.add(stats.docs());
            }
        } else {
            docs.add(stats.docs());
        }
        if (store == null) {
            if (stats.store() != null) {
                store = new StoreStats();
                store.add(stats.store());
            }
        } else {
            store.add(stats.store());
        }
        if (indexing == null) {
            if (stats.indexing() != null) {
                indexing = new IndexingStats();
                indexing.add(stats.indexing());
            }
        } else {
            indexing.add(stats.indexing());
        }
        if (get == null) {
            if (stats.get() != null) {
                get = new GetStats();
                get.add(stats.get());
            }
        } else {
            get.add(stats.get());
        }
        if (search == null) {
            if (stats.search() != null) {
                search = new SearchStats();
                search.add(stats.search());
            }
        } else {
            search.add(stats.search());
        }
        if (merge == null) {
            if (stats.merge() != null) {
                merge = new MergeStats();
                merge.add(stats.merge());
            }
        } else {
            merge.add(stats.merge());
        }
        if (refresh == null) {
            if (stats.refresh() != null) {
                refresh = new RefreshStats();
                refresh.add(stats.refresh());
            }
        } else {
            refresh.add(stats.refresh());
        }
        if (flush == null) {
            if (stats.flush() != null) {
                flush = new FlushStats();
                flush.add(stats.flush());
            }
        } else {
            flush.add(stats.flush());
        }
        if (warmer == null) {
            if (stats.warmer() != null) {
                warmer = new WarmerStats();
                warmer.add(stats.warmer());
            }
        } else {
            warmer.add(stats.warmer());
        }
    }

    @Nullable
    public DocsStats docs() {
        return this.docs;
    }

    @Nullable
    public DocsStats getDocs() {
        return this.docs;
    }

    @Nullable
    public StoreStats store() {
        return store;
    }

    @Nullable
    public StoreStats getStore() {
        return store;
    }

    @Nullable
    public IndexingStats indexing() {
        return indexing;
    }

    @Nullable
    public IndexingStats getIndexing() {
        return indexing;
    }

    @Nullable
    public GetStats get() {
        return get;
    }

    @Nullable
    public GetStats getGet() {
        return get;
    }

    @Nullable
    public SearchStats search() {
        return search;
    }

    @Nullable
    public SearchStats getSearch() {
        return search;
    }

    @Nullable
    public MergeStats merge() {
        return merge;
    }

    @Nullable
    public MergeStats getMerge() {
        return merge;
    }

    @Nullable
    public RefreshStats refresh() {
        return refresh;
    }

    @Nullable
    public RefreshStats getRefresh() {
        return refresh;
    }

    @Nullable
    public FlushStats flush() {
        return flush;
    }

    @Nullable
    public FlushStats getFlush() {
        return flush;
    }

    @Nullable
    public WarmerStats warmer() {
        return this.warmer;
    }

    @Nullable
    public WarmerStats getWarmer() {
        return this.warmer;
    }

    public static CommonStats readCommonStats(StreamInput in) throws IOException {
        CommonStats stats = new CommonStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            docs = DocsStats.readDocStats(in);
        }
        if (in.readBoolean()) {
            store = StoreStats.readStoreStats(in);
        }
        if (in.readBoolean()) {
            indexing = IndexingStats.readIndexingStats(in);
        }
        if (in.readBoolean()) {
            get = GetStats.readGetStats(in);
        }
        if (in.readBoolean()) {
            search = SearchStats.readSearchStats(in);
        }
        if (in.readBoolean()) {
            merge = MergeStats.readMergeStats(in);
        }
        if (in.readBoolean()) {
            refresh = RefreshStats.readRefreshStats(in);
        }
        if (in.readBoolean()) {
            flush = FlushStats.readFlushStats(in);
        }
        if (in.readBoolean()) {
            warmer = WarmerStats.readWarmerStats(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (docs == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            docs.writeTo(out);
        }
        if (store == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            store.writeTo(out);
        }
        if (indexing == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            indexing.writeTo(out);
        }
        if (get == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            get.writeTo(out);
        }
        if (search == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            search.writeTo(out);
        }
        if (merge == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            merge.writeTo(out);
        }
        if (refresh == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            refresh.writeTo(out);
        }
        if (flush == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            flush.writeTo(out);
        }
        if (warmer == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            warmer.writeTo(out);
        }
    }

    // note, requires a wrapping object
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (docs != null) {
            docs.toXContent(builder, params);
        }
        if (store != null) {
            store.toXContent(builder, params);
        }
        if (indexing != null) {
            indexing.toXContent(builder, params);
        }
        if (get != null) {
            get.toXContent(builder, params);
        }
        if (search != null) {
            search.toXContent(builder, params);
        }
        if (merge != null) {
            merge.toXContent(builder, params);
        }
        if (refresh != null) {
            refresh.toXContent(builder, params);
        }
        if (flush != null) {
            flush.toXContent(builder, params);
        }
        if (warmer != null) {
            warmer.toXContent(builder, params);
        }
        return builder;
    }
}
