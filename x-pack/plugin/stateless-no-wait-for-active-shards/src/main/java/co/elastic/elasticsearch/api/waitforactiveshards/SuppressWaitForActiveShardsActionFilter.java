/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.api.waitforactiveshards;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActiveShardCount;

/**
 * {@link ActionFilter} which removes the {@code ?wait_for_active_shards} parameter from indexing requests since it is not meaningful in
 * serverless. We may decide to do something different with this parameter once we have a general facility for manipulating REST parameters
 * in <a href="https://elasticco.atlassian.net/browse/ES-7547">ES-7547</a>, and in that case this whole module can probably just be removed.
 */
public class SuppressWaitForActiveShardsActionFilter extends ActionFilter.Simple {
    @Override
    public int order() {
        return 0;
    }

    @Override
    protected boolean apply(String action, ActionRequest request, ActionListener<?> listener) {
        if (action.equals(BulkAction.NAME) && request instanceof BulkRequest bulkRequest) {
            // Single-item write requests become bulk requests in TransportSingleItemBulkWriteAction so this catches them all
            bulkRequest.waitForActiveShards(ActiveShardCount.NONE);
        }
        return true;
    }
}
