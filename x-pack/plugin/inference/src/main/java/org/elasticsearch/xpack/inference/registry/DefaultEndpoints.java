/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.registry;

import org.elasticsearch.xpack.inference.services.elser.ElserInternalService;

import java.util.Set;

public final class DefaultEndpoints {

    public static final Set<String> DEFAULT_IDS = Set.of(ElserInternalService.DEFAULT_ELSER_ID);

    private DefaultEndpoints() {}
}
