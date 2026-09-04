/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.crossproject;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.core.Nullable;

/**
 * As part of Cross Project Search, we fanout certain requests to linked projects to examine what
 * indices exist on it. This is to ensure that an index is found on at least one project. For MRT=true,
 * this is a {@code ResolveIndexAction.Request} while for MRT=false, it's a {@code SearchShardsRequest}.
 * This structure is used to hold such a planning phase's result.
 * @param response A response from an endpoint used for the planning phase.
 * @param error An error that prevented us from communicating with a linked project.
 */
public record SearchPlanningPhaseResolutionResult(@Nullable ActionResponse response, @Nullable Exception error) {}
