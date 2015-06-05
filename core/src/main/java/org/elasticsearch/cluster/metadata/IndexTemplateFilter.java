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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;

/**
 * Enables filtering the index templates that will be applied for an index, per create index request.
 */
public interface IndexTemplateFilter {

    /**
     * @return  {@code true} if the given template should be applied on the newly created index,
     *          {@code false} otherwise.
     */
    boolean apply(CreateIndexClusterStateUpdateRequest request, IndexTemplateMetaData template);

    static class Compound implements IndexTemplateFilter {

        private IndexTemplateFilter[] filters;

        Compound(IndexTemplateFilter... filters) {
            this.filters = filters;
        }

        @Override
        public boolean apply(CreateIndexClusterStateUpdateRequest request, IndexTemplateMetaData template) {
            for (IndexTemplateFilter filter : filters) {
                if (!filter.apply(request, template)) {
                    return false;
                }
            }
            return true;
        }
    }
}
