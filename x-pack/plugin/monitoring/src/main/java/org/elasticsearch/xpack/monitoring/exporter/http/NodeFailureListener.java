/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.sniff.Sniffer;
import org.elasticsearch.core.Nullable;

/**
 * {@code NodeFailureListener} logs warnings for any node failure, but it can also notify a {@link Sniffer} and/or {@link HttpResource}
 * upon failures as well.
 * <p>
 * The {@linkplain #setSniffer(Sniffer) sniffer} and {@linkplain #setResource(HttpResource) resource} are expected to be set immediately
 * or not at all.
 */
class NodeFailureListener extends RestClient.FailureListener {

    private static final Logger logger = LogManager.getLogger(NodeFailureListener.class);

    /**
     * The optional {@link Sniffer} associated with the {@link RestClient}.
     */
    @Nullable
    private final SetOnce<Sniffer> snifferHolder = new SetOnce<>();
    /**
     * The optional {@link HttpResource} associated with the {@link RestClient}.
     */
    @Nullable
    private final SetOnce<HttpResource> resourceHolder = new SetOnce<>();

    /**
     * Get the {@link Sniffer} that is notified upon node failure.
     *
     * @return Can be {@code null}.
     */
    @Nullable
    public Sniffer getSniffer() {
        return snifferHolder.get();
    }

    /**
     * Set the {@link Sniffer} that is notified upon node failure.
     *
     * @param sniffer The sniffer to notify
     * @throws SetOnce.AlreadySetException if called more than once
     */
    public void setSniffer(@Nullable final Sniffer sniffer) {
        this.snifferHolder.set(sniffer);
    }

    /**
     * Get the {@link HttpResource} that is notified upon node failure.
     *
     * @return Can be {@code null}.
     */
    @Nullable
    public HttpResource getResource() {
        return resourceHolder.get();
    }

    /**
     * Set the {@link HttpResource} that is notified upon node failure.
     *
     * @param resource The resource to notify
     * @throws SetOnce.AlreadySetException if called more than once
     */
    public void setResource(@Nullable final HttpResource resource) {
        this.resourceHolder.set(resource);
    }

    @Override
    public void onFailure(final Node node) {
        HttpHost host = node.getHost();
        logger.warn("connection failed to node at [{}://{}:{}]", host.getSchemeName(), host.getHostName(), host.getPort());

        final HttpResource resource = this.resourceHolder.get();
        final Sniffer sniffer = this.snifferHolder.get();

        if (resource != null) {
            resource.markDirty();
        }
        if (sniffer != null) {
            sniffer.sniffOnFailure();
        }
    }

}
