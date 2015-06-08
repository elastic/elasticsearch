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

package org.elasticsearch.index.deletionpolicy;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class KeepLastNDeletionPolicy extends AbstractESDeletionPolicy {

    private final int numToKeep;

    @Inject
    public KeepLastNDeletionPolicy(ShardId shardId, @IndexSettings Settings indexSettings) {
        super(shardId, indexSettings);
        this.numToKeep = indexSettings.getAsInt("index.deletionpolicy.num_to_keep", 5);
        logger.debug("Using [keep_last_n] deletion policy with num_to_keep[{}]", numToKeep);
    }

    @Override
    public void onInit(List<? extends IndexCommit> commits) throws IOException {
        // do no deletions on init
        doDeletes(commits);
    }

    @Override
    public void onCommit(List<? extends IndexCommit> commits) throws IOException {
        doDeletes(commits);
    }

    private void doDeletes(List<? extends IndexCommit> commits) {
        int size = commits.size();
        for (int i = 0; i < size - numToKeep; i++) {
            commits.get(i).delete();
        }
    }

}
