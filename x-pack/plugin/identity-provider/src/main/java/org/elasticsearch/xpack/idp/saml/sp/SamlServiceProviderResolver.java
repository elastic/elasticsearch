/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.sp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex.DocumentSupplier;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex.DocumentVersion;

import java.util.Objects;
import java.util.stream.Collectors;

public class SamlServiceProviderResolver implements ClusterStateListener {

    private final Cache<String, CachedServiceProvider> cache;
    private final SamlServiceProviderIndex index;
    private final SamlServiceProviderFactory serviceProviderFactory;

    public SamlServiceProviderResolver(
        Settings settings,
        SamlServiceProviderIndex index,
        SamlServiceProviderFactory serviceProviderFactory
    ) {
        this.cache = ServiceProviderCacheSettings.buildCache(settings);
        this.index = index;
        this.serviceProviderFactory = serviceProviderFactory;
    }

    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * Find a {@link SamlServiceProvider} by entity-id.
     *
     * @param listener Callback for the service provider object. Calls {@link ActionListener#onResponse} with a {@code null} value if the
     *                 service provider does not exist.
     */
    public void resolve(String entityId, ActionListener<SamlServiceProvider> listener) {
        index.findByEntityId(entityId, listener.delegateFailureAndWrap((delegate, documentSuppliers) -> {
            if (documentSuppliers.isEmpty()) {
                delegate.onResponse(null);
                return;
            }
            if (documentSuppliers.size() > 1) {
                delegate.onFailure(
                    new IllegalStateException(
                        "Found multiple service providers with entity ID ["
                            + entityId
                            + "] - document ids ["
                            + documentSuppliers.stream().map(s -> s.version.id).collect(Collectors.joining(","))
                            + "] in index ["
                            + index
                            + "]"
                    )
                );
                return;
            }
            final DocumentSupplier doc = Iterables.get(documentSuppliers, 0);
            final CachedServiceProvider cached = cache.get(entityId);
            if (cached != null && cached.documentVersion.equals(doc.version)) {
                delegate.onResponse(cached.serviceProvider);
            } else {
                populateCacheAndReturn(entityId, doc, delegate);
            }
        }));
    }

    private void populateCacheAndReturn(String entityId, DocumentSupplier doc, ActionListener<SamlServiceProvider> listener) {
        final SamlServiceProvider serviceProvider = serviceProviderFactory.buildServiceProvider(doc.document.get());
        final CachedServiceProvider cacheEntry = new CachedServiceProvider(entityId, doc.version, serviceProvider);
        cache.put(entityId, cacheEntry);
        listener.onResponse(serviceProvider);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final Index previousIndex = index.getIndex(event.previousState());
        final Index currentIndex = index.getIndex(event.state());
        if (Objects.equals(previousIndex, currentIndex) == false) {
            logger.info("Index has changed [{}] => [{}], clearing cache", previousIndex, currentIndex);
            this.cache.invalidateAll();
        }
    }

    private class CachedServiceProvider {
        private final String entityId;
        private final DocumentVersion documentVersion;
        private final SamlServiceProvider serviceProvider;

        private CachedServiceProvider(String entityId, DocumentVersion documentVersion, SamlServiceProvider serviceProvider) {
            this.entityId = entityId;
            this.documentVersion = documentVersion;
            this.serviceProvider = serviceProvider;
        }
    }
}
