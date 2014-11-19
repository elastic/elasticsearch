/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.index.cache.filter;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilterCachingPolicy;
import org.apache.lucene.search.UsageTrackingFilterCachingPolicy;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;

/**
 * This class is a wrapper around {@link UsageTrackingFilterCachingPolicy}
 * which wires parameters through index settings and makes sure to not
 * cache {@link DocIdSet}s which have a {@link DocIdSets#isBroken(DocIdSetIterator) broken}
 * iterator.
 */
public class AutoFilterCachingPolicy extends AbstractIndexComponent implements FilterCachingPolicy {

    // These settings don't have the purpose of being documented. They are only here so that
    // if anyone ever hits an issue with elasticsearch that is due to the value of one of these
    // parameters, then it might be possible to temporarily work around the issue without having
    // to wait for a new release

    // number of times a filter that produces cacheable filters should be seen before the doc id sets are cached
    public static final String MIN_FREQUENCY_COSTLY = "index.cache.filter.policy.min_frequency.costly";
    // number of times a filter that produces cacheable filters should be seen before the doc id sets are cached
    public static final String MIN_FREQUENCY_CACHEABLE = "index.cache.filter.policy.min_frequency.cacheable";
    // same for filters that produce doc id sets that are not directly cacheable
    public static final String MIN_FREQUENCY_OTHER = "index.cache.filter.policy.min_frequency.other";
    // sources of segments that should be cached
    public static final String MIN_SEGMENT_SIZE_RATIO = "index.cache.filter.policy.min_segment_size_ratio";
    // size of the history to keep for filters. A filter will be cached if it has been seen more than a given
    // number of times (depending on the filter, the segment and the produced DocIdSet) in the most
    // ${history_size} recently used filters
    public static final String HISTORY_SIZE = "index.cache.filter.policy.history_size";

    public static Settings AGGRESSIVE_CACHING_SETTINGS = ImmutableSettings.builder()
            .put(MIN_FREQUENCY_CACHEABLE, 1)
            .put(MIN_FREQUENCY_COSTLY, 1)
            .put(MIN_FREQUENCY_OTHER, 1)
            .put(MIN_SEGMENT_SIZE_RATIO, 0.000000001f)
            .build();

    private final FilterCachingPolicy in;

    @Inject
    public AutoFilterCachingPolicy(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
        final int historySize = indexSettings.getAsInt(HISTORY_SIZE, 1000);
        // cache aggressively filters that produce sets that are already cacheable,
        // ie. if the filter has been used twice or more among the most 1000 recently
        // used filters
        final int minFrequencyCacheable = indexSettings.getAsInt(MIN_FREQUENCY_CACHEABLE, 2);
        // cache aggressively filters whose getDocIdSet method is costly
        final int minFrequencyCostly = indexSettings.getAsInt(MIN_FREQUENCY_COSTLY, 2);
        // be a bit less aggressive when the produced doc id sets are not cacheable
        final int minFrequencyOther = indexSettings.getAsInt(MIN_FREQUENCY_OTHER, 5);
        final float minSegmentSizeRatio = indexSettings.getAsFloat(MIN_SEGMENT_SIZE_RATIO, 0.01f);
        in = new UsageTrackingFilterCachingPolicy(minSegmentSizeRatio, historySize, minFrequencyCostly, minFrequencyCacheable, minFrequencyOther);
    }

    @Override
    public void onCache(Filter filter) {
        in.onCache(filter);
    }

    @Override
    public boolean shouldCache(Filter filter, LeafReaderContext context, DocIdSet set) throws IOException {
        if (set != null && DocIdSets.isBroken(set.iterator())) {
            // O(maxDoc) to cache, no thanks.
            return false;
        }

        return in.shouldCache(filter, context, set);
    }

}
