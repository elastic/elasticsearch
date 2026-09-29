/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.plugins.Plugin;

/**
 * Keeps a small, continuously maintained sample of live kNN queries so that production recall can be
 * estimated without replaying all traffic. The pipeline runs on the coordinating node and is designed
 * to stay off the search critical path.
 */
public class QuerySamplingPlugin extends Plugin {}
