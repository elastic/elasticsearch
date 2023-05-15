/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reservedstate.NonStateTransformResult;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.action.rolemapping.ReservedRoleMappingAction;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * A test class that allows us to Inject new type of Reserved Handler that can
 * simulate errors in saving role mappings.
 * <p>
 * We can't use our regular path to simply make an extension of LocalStateSecurity
 * in an integration test class, because the reserved handlers are injected through
 * SPI. (see {@link LocalReservedUnstableSecurityStateHandlerProvider})
 */
public class UnstableLocalStateSecurity extends LocalStateSecurity {

    public UnstableLocalStateSecurity(Settings settings, Path configPath) throws Exception {
        super(settings, configPath);
        // We reuse most of the initialization of LocalStateSecurity, we then just overwrite
        // the security plugin with an extra method to give us a fake RoleMappingAction.
        Optional<Plugin> security = plugins.stream().filter(p -> p instanceof Security).findFirst();
        if (security.isPresent()) {
            plugins.remove(security.get());
        }

        UnstableLocalStateSecurity thisVar = this;
        var action = new ReservedUnstableRoleMappingAction();

        plugins.add(new Security(settings, super.securityExtensions()) {
            @Override
            protected SSLService getSslService() {
                return thisVar.getSslService();
            }

            @Override
            protected XPackLicenseState getLicenseState() {
                return thisVar.getLicenseState();
            }

            @Override
            List<ReservedClusterStateHandler<?>> reservedClusterStateHandlers() {
                // pretend the security index is initialized after 2 seconds
                var timer = new java.util.Timer();
                timer.schedule(new java.util.TimerTask() {
                    @Override
                    public void run() {
                        action.securityIndexRecovered();
                        timer.cancel();
                    }
                }, 2_000);
                return List.of(action);
            }
        });
    }

    public static class ReservedUnstableRoleMappingAction extends ReservedRoleMappingAction {
        /**
         * Creates a fake ReservedRoleMappingAction that doesn't actually use the role mapping store
         */
        public ReservedUnstableRoleMappingAction() {
            // we don't actually need a NativeRoleMappingStore
            super(null);
        }

        /**
         * The nonStateTransform method is the only one that uses the native store, we simply pretend
         * something has called the onFailure method of the listener.
         */
        @Override
        protected void nonStateTransform(
            Collection<PutRoleMappingRequest> requests,
            TransformState prevState,
            ActionListener<NonStateTransformResult> listener
        ) {
            listener.onFailure(new IllegalStateException("Fake exception"));
        }
    }
}
