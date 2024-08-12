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

package co.elastic.elasticsearch.stateless.xpack;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.rollup.action.GetRollupIndexCapsAction;

/**
 * ML's <code>DatafeedManager</code> invokes this api, so we need to return an empty response.
 */
public class DummyTransportGetRollupIndexCapsAction extends HandledTransportAction<
    GetRollupIndexCapsAction.Request,
    GetRollupIndexCapsAction.Response> {

    @Inject
    public DummyTransportGetRollupIndexCapsAction(TransportService transportService, ActionFilters actionFilters) {
        super(
            GetRollupIndexCapsAction.NAME,
            transportService,
            actionFilters,
            GetRollupIndexCapsAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
    }

    @Override
    protected void doExecute(
        Task task,
        GetRollupIndexCapsAction.Request request,
        ActionListener<GetRollupIndexCapsAction.Response> listener
    ) {
        listener.onResponse(new GetRollupIndexCapsAction.Response());
    }
}
