/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.Collections;
import java.util.function.Predicate;

public final class SystemPrivilege extends Privilege {

    public static SystemPrivilege INSTANCE = new SystemPrivilege();

    private static final Predicate<String> UNDERLYING = Automatons.predicate(Automatons.
        minusAndMinimize(Automatons.patterns(
            "internal:*",
            "indices:monitor/*", // added for monitoring
            "cluster:monitor/*",  // added for monitoring
            "cluster:admin/bootstrap/*", // for the bootstrap service
            "cluster:admin/reroute", // added for DiskThresholdDecider.DiskListener
            "indices:admin/mapping/put", // needed for recovery and shrink api
            "indices:admin/template/put", // needed for the TemplateUpgradeService
            "indices:admin/template/delete", // needed for the TemplateUpgradeService
            "indices:admin/seq_no/global_checkpoint_sync*", // needed for global checkpoint syncs
            "indices:admin/settings/update" // needed for DiskThresholdMonitor.markIndicesReadOnly
        ), Automatons.patterns(
            // TODO: Determine how to handle CCR security for proxy actions
//        "internal:transport/proxy/*"
        )));

    private static final Predicate<String> PREDICATE = s -> {
        if (TransportActionProxy.isProxyAction(s)) {
            return UNDERLYING.test(TransportActionProxy.unwrapAction(s));
        } else {
            return UNDERLYING.test(s);
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
