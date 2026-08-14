/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.resourceexhaustion;

import org.elasticsearch.plugins.Plugin;

/**
 * Marker plugin for the resource-exhaustion test module. This module tests that Elasticsearch
 * returns 429 responses under resource exhaustion (circuit breakers, thread pool saturation, etc.)
 * using only built-in Elasticsearch mechanisms — no server-side injection is needed.
 */
public class ResourceExhaustionPlugin extends Plugin {}
