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
package org.elasticsearch.client;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class IndexClient {

    private final RestClient client;

    public IndexClient(RestClient client) {
        this.client = client;
    }

    public void delete(String index, String type, String id) throws IOException {
        delete(index, type, id, null);
    }
    public void delete(String index, String type, String id, DeleteOptions params) throws IOException {
        Objects.requireNonNull(index, "index must not be null");
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(id, "id must not be null");
        String deleteEndpoint = String.format(Locale.ROOT, "/%s/%s/%s", index, type, id);
        client.httpDelete(deleteEndpoint, params == null ? Collections.emptyMap() : params.options);
    }

    public class DeleteOptions {
        private final Map<String, Object> options = new HashMap<>();
        /** Specific write consistency setting for the operation one of "one", "quorum", "all"*/
        public void consistency(String consistency) {
            options.put("consistency", consistency);
        };
        /** ID of parent document */
        public void parent(String parent){
            options.put("parent", parent);
        };
        /** Refresh the index after performing the operation */
        public void refresh(Boolean refresh) {
            options.put("refresh", refresh);
        };
        /** Specific routing value */
        public void routing(String routing) {
            options.put("routing", routing);
        };
        /** Explicit version number for concurrency control */
        public void version(Number version) {
            options.put("version", version);
        };
        /** Specific version type one of "internal", "external", "external_gte", "force" */
        public void versionType(String versionType) {
            options.put("version_type", versionType);
        };
        /** Explicit operation timeout */
        public void timeout(String timeout) {
            options.put("timeout", timeout);
        };
    }
}
