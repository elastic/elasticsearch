/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.codec.profiler;

/**
 * Records whether a CPU reading failed. A collector and its details share one instance, so a failed reading removes the
 * CPU time from all of them. It has the one writer of those collectors. A snapshot that races the writer breaks the
 * contract of {@link ProfileCollector} and can see this flag out of step with the timers.
 */
final class CpuState {
    boolean failed;
}
