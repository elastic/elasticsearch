/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.merge.policy;

import com.google.inject.AbstractModule;
import org.apache.lucene.index.LogMergePolicy;
import org.elasticsearch.index.shard.IndexShardLifecycle;
import org.elasticsearch.util.settings.Settings;

/**
 * @author kimchy (Shay Banon)
 */
@IndexShardLifecycle
public class MergePolicyModule extends AbstractModule {

    private final Settings settings;

    public MergePolicyModule(Settings settings) {
        this.settings = settings;
    }

    @Override protected void configure() {
        bind(Integer.class)
                .annotatedWith(MergeFactor.class)
                .toInstance(settings.getAsInt("index.merge.policy.mergeFactor", LogMergePolicy.DEFAULT_MERGE_FACTOR));

        // TODO consider moving to BalancedSegmentMergePolicyProvider as the default
        // Note, when using the index jmeter benchmark, it seams like the balanced merger keeps on merging ...
        // don't have time to look at it now...
        bind(MergePolicyProvider.class)
                .to(settings.getAsClass("index.merge.policy.type", LogByteSizeMergePolicyProvider.class))
                .asEagerSingleton();
    }
}
