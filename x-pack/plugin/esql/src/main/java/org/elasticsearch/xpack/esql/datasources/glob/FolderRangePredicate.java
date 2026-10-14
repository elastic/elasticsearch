/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import java.util.List;
import java.util.Map;

/**
 * Joint temporal overlap test for a folder's bound keys. {@code false} only when the folder cannot hold a
 * matching file. Unparseable folder values, invalid dates, and keys not in the detected partitions stay
 * ({@code true}): listing a superset is always correct.
 *
 * <p>Grain {@code IN} lists stay independent; this only narrows. The walk must not fake per-key IN lists
 * from the joint predicate — it is path-local, while today's matcher is level-wide.
 */
public interface FolderRangePredicate {

    /**
     * {@code false} only when this folder cannot hold a matching file. {@code boundKeyValues} are first-wins
     * path bindings (outermost key only).
     */
    boolean mayContain(Map<String, String> boundKeyValues);

    /**
     * Stable cache-key fragment: spec + bind keys + quantized bounds. Finest grain among usable binds;
     * widened lower rounds down to that grain, upper rounds up. {@code NOW() - 1 day} shares one entry
     * per UTC day.
     */
    String identity();

    /**
     * Path keys the predicate actually uses ({@code year}/{@code month}/{@code day}/{@code hour}). Seeds the
     * walk's pending set so a globstar one-sided query does not withdraw at the first unhinted level.
     */
    List<String> bindKeys();
}
