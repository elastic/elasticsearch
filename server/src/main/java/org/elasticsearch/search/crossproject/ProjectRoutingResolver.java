/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.crossproject;

/**
 * Filter for the target projects based on the provided project routing string.
 */
public interface ProjectRoutingResolver {

    /**
     * The reserved term for representing the origin project in project routing.
     */
    String ORIGIN = "_origin";

    /**
     * Filters the specified TargetProjects based on the provided project routing string
     * @param projectRouting the project_routing specified in the request object
     * @param targetProjects The target projects to be filtered
     * @return A new TargetProjects instance containing only the projects that match the project routing.
     */
    TargetProjects resolve(String projectRouting, TargetProjects targetProjects);
}
