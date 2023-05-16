/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * <h2>T-Digest library</h2>
 * This package contains a fork for the [T-Digest](https://github.com/tdunning/t-digest) library that's used for percentile calculations.
 *
 * Forking the library allowed addressing bugs and inaccuracies around both the AVL- and the merging-based implementations and switching
 * from the former to the latter within TSDB, with substantial performance gains (10-50x). It also unlocks the use of BigArrays and other
 * ES-specific functionality to account for resources used in the digest data structures.
 */

package org.elasticsearch.tdigest;
