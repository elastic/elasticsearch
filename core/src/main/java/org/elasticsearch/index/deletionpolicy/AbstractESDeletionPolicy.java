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

import org.apache.lucene.index.IndexDeletionPolicy;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.IndexShardComponent;
import org.elasticsearch.index.shard.ShardId;

abstract class AbstractESDeletionPolicy extends IndexDeletionPolicy implements IndexShardComponent {

    protected final ESLogger logger;

    protected final ShardId shardId;

    protected final Settings indexSettings;

    protected AbstractESDeletionPolicy(ShardId shardId, @IndexSettings Settings indexSettings) {
        this.shardId = shardId;
        this.indexSettings = indexSettings;
        this.logger = Loggers.getLogger(getClass(), indexSettings, shardId);
    }

    @Override
    public ShardId shardId() {
        return this.shardId;
    }

    @Override
    public Settings indexSettings() {
        return this.indexSettings;
    }

    public String nodeName() {
        return indexSettings.get("name", "");
    }

    

}
