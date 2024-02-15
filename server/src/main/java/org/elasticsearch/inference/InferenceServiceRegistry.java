/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface InferenceServiceRegistry extends Closeable {
    void init(Client client);

    Map<String, InferenceService> getServices();

    Optional<InferenceService> getService(String serviceName);

    List<NamedWriteableRegistry.Entry> getNamedWriteables();

    class NoopInferenceServiceRegistry implements InferenceServiceRegistry {
        public NoopInferenceServiceRegistry() {}

        @Override
        public void init(Client client) {}

        @Override
        public Map<String, InferenceService> getServices() {
            return Map.of();
        }

        @Override
        public Optional<InferenceService> getService(String serviceName) {
            return Optional.empty();
        }

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return List.of();
        }

        @Override
        public void close() throws IOException {}
    }
}
