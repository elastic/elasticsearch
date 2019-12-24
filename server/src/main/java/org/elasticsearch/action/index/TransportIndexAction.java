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

package org.elasticsearch.action.index;

import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.bulk.TransportSingleItemBulkWriteAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.transport.TransportService;

/**
 * Performs the index operation.
 *
 * Allows for the following settings:
 * <ul>
 * <li><b>autoCreateIndex</b>: When set to {@code true}, will automatically create an index if one does not exists.
 * Defaults to {@code true}.
 * <li><b>allowIdGeneration</b>: If the id is set not, should it be generated. Defaults to {@code true}.
 * </ul>
 *
 * Deprecated use TransportBulkAction with a single item instead
 */
@Deprecated
public class TransportIndexAction extends TransportSingleItemBulkWriteAction<IndexRequest, IndexResponse> {

    @Inject
    public TransportIndexAction(ActionFilters actionFilters, TransportService transportService, TransportBulkAction bulkAction) {
        super(IndexAction.NAME, transportService, actionFilters, IndexRequest::new, bulkAction);
    }
}
