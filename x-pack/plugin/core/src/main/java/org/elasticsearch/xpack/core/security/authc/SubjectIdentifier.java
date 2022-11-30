/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.action.support.user.ActionUser;
import org.elasticsearch.common.util.LazyMap;

import java.util.Map;
import java.util.function.Supplier;

public final class SubjectIdentifier implements ActionUser.Id {

    private final String stringId;
    private final LazyMap<String, String> ecsMap;

    public SubjectIdentifier(String stringId, Supplier<Map<String, String>> ecs) {
        this.stringId = stringId;
        this.ecsMap = new LazyMap<>(ecs);
    }

    @Override
    public String toString() {
        return stringId;
    }

    @Override
    public Map<String, String> asEcsMap() {
        return ecsMap;
    }
}
