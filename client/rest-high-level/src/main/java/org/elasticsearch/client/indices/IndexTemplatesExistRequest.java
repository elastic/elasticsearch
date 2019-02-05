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

package org.elasticsearch.client.indices;

import java.util.Arrays;
import java.util.List;

/**
 * A request to check for the existence of index templates
 */
public class IndexTemplatesExistRequest extends GetIndexTemplatesRequest {

    /**
     * Create a request to check for the existence of index templates. At least one template index name must be provided
     *
     * @param names the names of templates to check for the existence of
     */
    public IndexTemplatesExistRequest(String... names) {
        this(Arrays.asList(names));
    }

    /**
     * Create a request to check for the existence of index templates. At least one template index name must be provided
     *
     * @param names the names of templates to check for the existence of
     */
    public IndexTemplatesExistRequest(List<String> names) {
        super(names);
        if (names().isEmpty()) {
            throw new IllegalArgumentException("must provide at least one index template name");
        }
    }
}
