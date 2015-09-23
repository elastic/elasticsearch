/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter.local;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.marvel.agent.exporter.Exporter;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.shield.SecuredClient;

import java.util.Collection;

/**
 *
 */
public class LocalExporter extends Exporter {

    public static final String TYPE = "local";

    private final Client client;

    public LocalExporter(Exporter.Config config, SecuredClient client) {
        super(TYPE, config);
        this.client = client;
    }

    @Override
    public void export(Collection<MarvelDoc> marvelDocs) {
    }

    @Override
    public void close() {
    }

    public static class Factory extends Exporter.Factory<LocalExporter> {

        private final SecuredClient client;

        @Inject
        public Factory(SecuredClient client) {
            super(TYPE, true);
            this.client = client;
        }

        @Override
        public LocalExporter create(Config config) {
            return new LocalExporter(config, client);
        }
    }
}
