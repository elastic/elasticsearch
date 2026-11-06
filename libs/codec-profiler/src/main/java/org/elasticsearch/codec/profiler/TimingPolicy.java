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
 * Which calls a {@link Timer} measures. Both policies count every call and use the same accounting: a measured call
 * stands for itself and for the unmeasured calls before it since the previous measured call. The reported time is the
 * sum of the measured times, each multiplied by the calls it stands for, plus an extrapolation for the calls after the
 * last measured call.
 *
 * <p>The policy is part of the timer definition. Two definitions of one identity with different policies are
 * different metrics, and their measurements do not merge.
 */
public enum TimingPolicy {
    /**
     * Measures every call. Each measured call stands only for itself. Between calls, the reported time is the sum of the
     * measured times and is not an estimate.
     */
    EXACT,
    /**
     * Measures the first 256 calls, then a decreasing share of the calls. After the first 256 calls, a call is measured
     * when the unmeasured calls since the last measured call reach the measured-up-to call count divided by 256, with a
     * cap of 1024. At the cap, one call in 1025 is measured. The search profile timer of Elasticsearch uses the same
     * schedule, so that the clock reads of a call made millions of times do not dominate its cost.
     *
     * <p>The reported time is an estimate when calls went unmeasured. The measured-call count describes coverage, not
     * accuracy. The estimate assumes that the measured calls represent the unmeasured calls near them. A cost that
     * changes over time, or a pattern that repeats in step with the gaps, can bias the estimate. No accuracy bound
     * applies.
     */
    ADAPTIVE
}
