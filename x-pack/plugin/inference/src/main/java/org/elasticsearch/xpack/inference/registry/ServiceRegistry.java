/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.registry;

import org.elasticsearch.xpack.inference.services.InferenceService;
import org.elasticsearch.xpack.inference.services.elser.ElserMlNodeService;

import java.util.Optional;

public class ServiceRegistry {

    ElserMlNodeService elserService;

    public ServiceRegistry(ElserMlNodeService elserService) {
        this.elserService = elserService;
    }

    public Optional<InferenceService> getService(String name) {
        if (name.equals(ElserMlNodeService.NAME)) {
            return Optional.of(elserService);
        }

        return Optional.empty();
    }

}
