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

import org.elasticsearch.common.Strings;

/**
 * A request to check for the existence of index templates
 */
public class ComposableIndexTemplateExistRequest extends GetComponentTemplatesRequest {

    /**
     * Create a request to check for the existence of index template. Name must be provided
     *
     * @param name the name of template to check for the existence of
     */
    public ComposableIndexTemplateExistRequest(String name) {
        super(name);
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("must provide index template name");
        }
    }
}
