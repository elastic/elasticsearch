/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.TransportVersion;

/**
 * A wrapper for objects to pass along the minimum transport version available in the cluster (and remotes),
 * esp. when dealing with listeners. Where this object gets consumed, we generally need to assume that all nodes in the cluster
 * (and remotes) are at the minimum transport version, so that we don't use features not supported everywhere.
 */
public record Versioned<T>(T inner, TransportVersion minimumVersion) {}
