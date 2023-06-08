/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolution;

import java.util.Set;

public record EnrichResolution(Set<EnrichPolicyResolution> resolvedPolicies, Set<String> existingPolicies) {}
