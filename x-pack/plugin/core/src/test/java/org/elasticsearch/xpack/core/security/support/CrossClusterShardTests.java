/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.support;

import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Binding;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege.CROSS_CLUSTER_REPLICATION;
import static org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege.CROSS_CLUSTER_REPLICATION_INTERNAL;
import static org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege.READ;
import static org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege.READ_CROSS_CLUSTER;
import static org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege.VIEW_METADATA;

public class CrossClusterShardTests extends ESSingleNodeTestCase {


    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Set.of(XPackPlugin.class); //TODO: need to move this test out to a QA test with a dependency on ALL plugins (including xpack plugins)
    }

    @SuppressWarnings("rawtypes")
    public void testMe() throws Exception {
        try (Node node = node()) {
            List<Binding<TransportAction>> transportActionBindings =
                node.injector().findBindingsByType(TypeLiteral.get(TransportAction.class));
            AtomicInteger actionCount = new AtomicInteger(0);
            AtomicInteger indexActionCount = new AtomicInteger(0);
            AtomicInteger matchingIndexActionCount = new AtomicInteger(0);
            AtomicInteger matchingIndexActionShardCount = new AtomicInteger(0);

            transportActionBindings.forEach(b -> {
                actionCount.incrementAndGet();
                TransportAction action = b.getProvider().get();
                String actionName = action.actionName;
                // System.out.println(actionName);
                if (actionName.startsWith("indices:")) {
                    indexActionCount.incrementAndGet();
                    //TODO: add an additional test to ensure that these names are in sync with the string version declared
                    // in CrossClusterApiKeyRoleDescriptorBuilder
                    if (READ.predicate().test(actionName)
                    || READ_CROSS_CLUSTER.predicate().test(actionName)
                    || VIEW_METADATA.predicate().test(actionName)
                    || CROSS_CLUSTER_REPLICATION.predicate().test(actionName)
                    || CROSS_CLUSTER_REPLICATION_INTERNAL.predicate().test(actionName)) {
                        //The set of all the index actions that permissions allow to cross cluster boundaries

                        matchingIndexActionCount.incrementAndGet();
                        if(action.getClass().getName().toLowerCase(Locale.ROOT).contains("shard")
                        || actionName.toLowerCase(Locale.ROOT).contains("shard")) {
                            // The set of all the index actions that are allowed to go cross cluster that have the word "shard"
                            matchingIndexActionShardCount.incrementAndGet();
                            System.out.println("********************");
                            System.out.println(action.getClass().getName());
                            System.out.println(actionName);
                            //TODO: check that these actions implement CrossClusterShardAction
                        }
                    }
                }

            });

            System.out.println(">>>>>>>>>>> actionCount: " + actionCount.get()
                + ", indexActionCount: " + indexActionCount.get()
                + ", matchingIndexActionCount: " + matchingIndexActionCount.get()
                + ", matchingIndexActionShardCount: " + matchingIndexActionShardCount.get());
        }
    }

}
