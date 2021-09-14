/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle;

import org.gradle.api.Project;

public class DistributionResolution {
    private Resolver resolver;
    private String name;
    private int priority;

    public DistributionResolution(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public Resolver getResolver() {
        return resolver;
    }

    public void setResolver(Resolver resolver) {
        this.resolver = resolver;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public int getPriority() {
        return priority;
    }

    public interface Resolver {
        DistributionDependency resolve(Project project, ElasticsearchDistribution distribution);
    }
}
