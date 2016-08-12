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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.migrate.TransportMigrateIndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.script.Script;

public class ReindexingDocumentMigrater implements TransportMigrateIndexAction.DocumentMigrater {
    @Override
    public void migrateDocuments(String sourceIndex, String destinationIndex, Script script, TimeValue timeout, Client client,
            ActionListener<Void> listener) {
        ReindexRequest reindex = new ReindexRequest(
                new SearchRequest(sourceIndex),
                new IndexRequest(destinationIndex));
        reindex.setScript(script);
        reindex.setTimeout(timeout);
        // Refresh so the documents are visible when we switch the aliases
        reindex.setRefresh(true); 
        // NOCOMMIT need to link up the status so the migration action's status looks like reindex's status while this is happening
        // NOCOMMIT should we prevent users from targeting a different index with this? probably
        client.execute(ReindexAction.INSTANCE, reindex, new ActionListener<BulkIndexByScrollResponse>() {
            @Override
            public void onResponse(BulkIndexByScrollResponse response) {
                if (false == response.getSearchFailures().isEmpty() || false == response.getBulkFailures().isEmpty()) {
                    throw new ElasticsearchException("Reindex failed " + response.getSearchFailures() + " " + response.getBulkFailures());
                }
                listener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
