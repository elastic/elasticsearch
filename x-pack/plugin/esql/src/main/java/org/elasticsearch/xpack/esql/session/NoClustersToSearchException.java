/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

/**
 * Sentinel exception indicating that logical planning could not find any clusters to search
 * when, for a remote-only cross-cluster search, all clusters have been marked as SKIPPED.
 * Intended for use only on the querying coordinating during ES|QL logical planning.
 */
public class NoClustersToSearchException extends RuntimeException {}
