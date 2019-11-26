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

package org.elasticsearch.index.reindex;

import org.elasticsearch.common.settings.Setting;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class ReindexHeaders {

    /**
     * A list of headers that should be extracted from the start reindex request and used on subsequent
     * requests when resilient reindexing is enabled. For example, any authorization headers required for
     * reindexing should be configured.
     */
    public static final Setting<List<String>> REINDEX_INCLUDED_HEADERS = Setting.listSetting("reindex.request_headers.include",
        Collections.emptyList(), Function.identity(), Setting.Property.NodeScope);
}
