/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.crossproject;

import java.util.List;

/**
 * Carries per-request project routing metadata from the resolver chain to the transport actions for telemetry recording.
 * Populated by the serverless cross-project resolver and attached to {@link TargetProjects}.
 *
 * <p>Custom-tag detection is left to consumers: a tag is custom if its name does not start with {@code _}.
 *
 * @param tagsUsedInRouting all tag names referenced in the resolved expression
 * @param usedNamedExpression true when the request used a named-expression ({@code @name}) reference
 * @param usedAliasWildcard true when the expression was exactly {@code _alias:*}
 * @param usedAliasOrigin true when the expression was exactly {@code _alias:_origin}
 */
public record ProjectRoutingRequestInfo(
    List<String> tagsUsedInRouting,
    boolean usedNamedExpression,
    boolean usedAliasWildcard,
    boolean usedAliasOrigin
) {
    public static final ProjectRoutingRequestInfo NONE = new ProjectRoutingRequestInfo(List.of(), false, false, false);
}
