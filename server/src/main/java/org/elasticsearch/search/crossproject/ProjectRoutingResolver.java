/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.search.crossproject;

/**
 * Filter for the target projects based on the provided project routing string.
 */
public interface ProjectRoutingResolver {

    /**
     * Filters the specified TargetProjects based on the provided project routing string
     * @param projectRouting the project_routing specified in the request object
     * @param targetProjects The target projects to be filtered
     * @return A new TargetProjects instance containing only the projects that match the project routing.
     */
    TargetProjects resolve(String projectRouting, TargetProjects targetProjects);
}
