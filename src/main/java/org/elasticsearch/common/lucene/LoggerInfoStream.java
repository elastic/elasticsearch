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

package org.elasticsearch.common.lucene;

import org.apache.lucene.util.InfoStream;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;

/** An InfoStream (for Lucene's IndexWriter) that redirects
 *  messages to Logger.trace. */

public final class LoggerInfoStream extends InfoStream {
    private final ESLogger logger;

    public LoggerInfoStream(Settings settings, ShardId shardId) {
        logger = Loggers.getLogger("lucene.iw", settings, shardId);
    }

    public void message(String component, String message) {
        logger.trace("{} {}: {}", Thread.currentThread().getName(), component, message);
    }
  
    public boolean isEnabled(String component) {
        // TP is a special "test point" component; we don't want
        // to log it:
        return logger.isTraceEnabled() && component.equals("TP") == false;
    }

    @Override
    public void close() {
    }
}
