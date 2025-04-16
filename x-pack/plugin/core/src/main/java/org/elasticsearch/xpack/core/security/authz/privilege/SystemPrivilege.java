/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.readonly.TransportAddIndexBlockAction;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.search.TransportSearchShardsAction;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.index.seqno.RetentionLeaseBackgroundSyncAction;
import org.elasticsearch.index.seqno.RetentionLeaseSyncAction;
import org.elasticsearch.persistent.CompletionPersistentTaskAction;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.xpack.core.security.action.ActionTypes;
import org.elasticsearch.xpack.core.security.support.StringMatcher;

import java.util.Collections;
import java.util.function.Predicate;

public final class SystemPrivilege extends Privilege {

    public static final SystemPrivilege INSTANCE = new SystemPrivilege();

    private static final Predicate<String> ALLOWED_ACTIONS = StringMatcher.of(
        "internal:*",
        "indices:monitor/*", // added for monitoring
        "cluster:monitor/*",  // added for monitoring
        "cluster:admin/bootstrap/*", // for the bootstrap service
        "cluster:admin/reroute", // added for DiskThresholdDecider.DiskListener
        "indices:admin/mapping/put", // needed for recovery and shrink api
        "indices:admin/mapping/auto_put", // needed for recovery and shrink api
        "indices:admin/template/put", // needed for the TemplateUpgradeService
        "indices:admin/template/delete", // needed for the TemplateUpgradeService
        "indices:admin/seq_no/global_checkpoint_sync*", // needed for global checkpoint syncs
        RetentionLeaseSyncAction.ACTION_NAME + "*", // needed for retention lease syncs
        RetentionLeaseBackgroundSyncAction.ACTION_NAME + "*", // needed for background retention lease syncs
        RetentionLeaseActions.ADD.name() + "*", // needed for CCR to add retention leases
        RetentionLeaseActions.REMOVE.name() + "*", // needed for CCR to remove retention leases
        RetentionLeaseActions.RENEW.name() + "*", // needed for CCR to renew retention leases
        "indices:admin/settings/update", // needed for: DiskThresholdMonitor.markIndicesReadOnly, SystemIndexMigrator
        CompletionPersistentTaskAction.INSTANCE.name(), // needed for ShardFollowTaskCleaner
        "indices:data/write/*", // needed for SystemIndexMigrator
        "indices:data/read/*", // needed for SystemIndexMigrator
        "indices:admin/refresh", // needed for SystemIndexMigrator
        "indices:admin/aliases", // needed for SystemIndexMigrator
        TransportCreateIndexAction.TYPE.name() + "*", // needed for SystemIndexMigrator
        TransportAddIndexBlockAction.TYPE.name() + "*", // needed for SystemIndexMigrator
        TransportUpdateSettingsAction.TYPE.name() + "*", // needed for SystemIndexMigrator
        TransportSearchShardsAction.TYPE.name(), // added so this API can be called with the system user by other APIs
        ActionTypes.RELOAD_REMOTE_CLUSTER_CREDENTIALS_ACTION.name() // needed for Security plugin reload of remote cluster credentials
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
