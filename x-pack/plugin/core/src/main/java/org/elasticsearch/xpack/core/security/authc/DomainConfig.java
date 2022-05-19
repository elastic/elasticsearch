/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import java.util.List;

public record DomainConfig(String name, List<String> memberRealmNames, boolean literalUsername, String suffix)
    implements
        Comparable<DomainConfig> {

    public DomainConfig {
        assert (literalUsername && suffix != null) || (false == literalUsername && suffix == null);
    }

    @Override
    public int compareTo(DomainConfig o) {
        return name.compareTo(o.name);
    }
}
