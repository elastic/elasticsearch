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
package org.elasticsearch.index.shard;

import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexSettings;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

public class IndexShardDeprecatedSettingTests extends IndexShardTestCase {
    @Override
    protected boolean enableWarningsCheck() {
        return false;
    }

    public void testCheckOnStartupDeprecatedValue() throws Exception {
        final Settings settings = Settings.builder().put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), "fix").build();

        try(ThreadContext threadContext = new ThreadContext(Settings.EMPTY)) {
            DeprecationLogger.setThreadContext(threadContext);
            final IndexShard newShard = newShard(true, settings);

            final Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
            final List<String> warnings = responseHeaders.get("Warning");
            assertThat(warnings.toString(), warnings, hasSize(1));
            assertThat(warnings.get(0), containsString("Setting [index.shard.check_on_startup] is set to deprecated value [fix], "
                + "which will be unsupported in future"));

            closeShards(newShard);
        }
    }
}
