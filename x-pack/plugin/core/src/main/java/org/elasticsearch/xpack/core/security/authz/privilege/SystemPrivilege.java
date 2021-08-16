/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.index.seqno.RetentionLeaseBackgroundSyncAction;
import org.elasticsearch.index.seqno.RetentionLeaseSyncAction;
import org.elasticsearch.persistent.CompletionPersistentTaskAction;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.xpack.core.security.support.StringMatcher;

import java.util.Collections;
import java.util.function.Predicate;

public final class SystemPrivilege extends Privilege {

    public static SystemPrivilege INSTANCE = new SystemPrivilege();

    private static final Predicate<String> ALLOWED_ACTIONS = StringMatcher.of(
        "internal:*",
        "indices:monitor/*", // added for monitoring
        "cluster:monitor/*",  // added for monitoring
        "cluster:admin/bootstrap/*", // for the bootstrap service
        "cluster:admin/reroute", // added for DiskThresholdDecider.DiskListener
        "indices:admin/mapping/put", // needed for recovery and shrink api
        "indices:admin/template/put", // needed for the TemplateUpgradeService
        "indices:admin/template/delete", // needed for the TemplateUpgradeService
        "indices:admin/seq_no/global_checkpoint_sync*", // needed for global checkpoint syncs
        RetentionLeaseSyncAction.ACTION_NAME + "*", // needed for retention lease syncs
        RetentionLeaseBackgroundSyncAction.ACTION_NAME + "*", // needed for background retention lease syncs
        RetentionLeaseActions.Add.ACTION_NAME + "*", // needed for CCR to add retention leases
        RetentionLeaseActions.Remove.ACTION_NAME + "*", // needed for CCR to remove retention leases
        RetentionLeaseActions.Renew.ACTION_NAME + "*", // needed for CCR to renew retention leases
        "indices:admin/settings/update", // needed for DiskThresholdMonitor.markIndicesReadOnly
        CompletionPersistentTaskAction.NAME, // needed for ShardFollowTaskCleaner

        // needed for feature reset with system user
        // TODO[wrb]: This seems better than allowing restricted indices in test user roles, but is there an even better approach?
        "cluster:internal/xpack/transform/reset_mode",
        "indices:admin/data_stream/delete",
        "cluster:internal/xpack/ml/reset_mode",
        "cluster:admin/transform/stop",
        "cluster:admin/xpack/ml/datafeed/stop",
        "cluster:internal/xpack/ml/job/kill/*",
        "cluster:admin/xpack/ml/job/close",
        "cluster:admin/xpack/ml/data_frame/analytics/stop",
        "indices:admin/delete"
    );

    private static final Predicate<String> PREDICATE = (action) -> {
        // Only allow a proxy action if the underlying action is allowed
        if (TransportActionProxy.isProxyAction(action)) {
            return ALLOWED_ACTIONS.test(TransportActionProxy.unwrapAction(action));
        } else {
            return ALLOWED_ACTIONS.test(action);
        }
    };

    private SystemPrivilege() {
        super(Collections.singleton("internal"));
    }

    @Override
    public Predicate<String> predicate() {
        return PREDICATE;
    }
}
