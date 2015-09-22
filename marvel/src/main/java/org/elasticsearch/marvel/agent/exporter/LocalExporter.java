/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.marvel.shield.SecuredClient;

import java.util.Collection;

/**
 *
 */
public class LocalExporter implements Exporter<LocalExporter> {

    public static final String NAME = "local";

    private final Client client;

    @Inject
    public LocalExporter(SecuredClient client) {
        this.client = client;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void export(Collection<MarvelDoc> marvelDocs) {

    }

    @Override
    public Lifecycle.State lifecycleState() {
        return null;
    }

    @Override
    public void addLifecycleListener(LifecycleListener lifecycleListener) {

    }

    @Override
    public void removeLifecycleListener(LifecycleListener lifecycleListener) {

    }

    @Override
    public LocalExporter start() {
        return null;
    }

    @Override
    public LocalExporter stop() {
        return null;
    }

    @Override
    public void close() {

    }
}
