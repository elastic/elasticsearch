/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.ql.index.IndexResolution;

public record EnrichPolicyResolution(String policyName, EnrichPolicy policy, IndexResolution index) {}
