/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.core.Releasable;

/**
 * Collections of "side channels" for the {@link Operator}s running on one node. These
 * nodes must run on the same JVM, but will not run in the same {@link Driver}.
 */
public class SideChannels implements Releasable {
}
