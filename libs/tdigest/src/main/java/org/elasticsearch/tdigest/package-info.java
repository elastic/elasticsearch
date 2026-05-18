/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * <h2>T-Digest library</h2>
 * This package contains a fork for the [T-Digest](https://github.com/tdunning/t-digest) library that's used for percentile calculations.
 *
 * Forking the library allows addressing bugs and inaccuracies around both the AVL- and the merging-based implementations that unblocks
 * switching from the former to the latter, with substantial performance gains. It also unlocks the use of BigArrays and other
 * ES-specific functionality to account for resources used in the digest data structures.
 */

package org.elasticsearch.tdigest;
