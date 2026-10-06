/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.xpack.esql.datasources.FileSetFingerprint;

/**
 * The address of the STATISTICS measured over a resolved file SET by one read: the memoized multi-file
 * fold for one dataset.
 * <p>
 * The subject is the set, so the set is the address: the 128-bit
 * {@link FileSetFingerprint} is a commutative fold over every file's path, mtime and size plus the file
 * count, which makes this correct-or-miss by construction - any file added, removed or modified derives
 * a different address, and the stale entry ages out by LRU. Deliberately NOT {@link FileAddressed}: a
 * per-file harvest must never reach a set-level record, and that is now a compile-time fact rather than
 * the {@code #dataset-agg} marker test it used to be.
 * <p>
 * It carries a {@link ReadDecision} for the same reason the per-file statistics address does, and today
 * the fold stamps {@link ReadDecision#UNKNOWN} into it. That is the state of the aggregate rail as it
 * stands, not a design: nothing compares the configuration that produced the fold against the one
 * consuming it, and it is not a wrong answer only because the strict multi-file rail never reaches the
 * aggregate, a non-strict overlay only retypes in place so a projection-less {@code COUNT(*)} sees the
 * same survivor set, and a projection-decided drop suppresses its publish at the producer. Change any
 * one of those and it becomes a silent wrong count. Having the component here is what lets stamping it
 * be a one-line change rather than a key migration.
 */
public record SetStatsKey(SourceScope scope, FileSetFingerprint fileSet, ReadDecision decision) {}
