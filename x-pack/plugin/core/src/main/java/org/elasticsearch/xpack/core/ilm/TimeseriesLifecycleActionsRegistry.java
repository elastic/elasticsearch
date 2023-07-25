/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.util.set.Sets;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Defines a record of the actions that are allowed for the timeseries lifecycle.
 * The order of actions is versioned, newer versions must contain all previous defined actions and are
 * allowed to make additive changes (we do not support removing actions, but actions to be removed
 * must be converted to no-ops)
 */
public final class TimeseriesLifecycleActionsRegistry {

    public static final int VERSION_ONE = 1;
    // moves the downsample action after the migrate action in the warm and cold phases
    public static final int VERSION_TWO = 2;
    public static final int CURRENT_VERSION = VERSION_TWO;

    public static final String HOT_PHASE = "hot";
    public static final String WARM_PHASE = "warm";
    public static final String COLD_PHASE = "cold";
    public static final String FROZEN_PHASE = "frozen";
    public static final String DELETE_PHASE = "delete";

    public static final Map<Integer, List<String>> ORDERED_VALID_HOT_ACTIONS = Map.of(
        VERSION_ONE,
        Stream.of(
            SetPriorityAction.NAME,
            UnfollowAction.NAME,
            RolloverAction.NAME,
            ReadOnlyAction.NAME,
            DownsampleAction.NAME,
            ShrinkAction.NAME,
            ForceMergeAction.NAME,
            SearchableSnapshotAction.NAME
        ).filter(Objects::nonNull).toList(),
        VERSION_TWO,
        Stream.of(
            SetPriorityAction.NAME,
            UnfollowAction.NAME,
            RolloverAction.NAME,
            ReadOnlyAction.NAME,
            DownsampleAction.NAME,
            ShrinkAction.NAME,
            ForceMergeAction.NAME,
            SearchableSnapshotAction.NAME
        ).filter(Objects::nonNull).toList()
    );

    public static final Map<Integer, List<String>> ORDERED_VALID_WARM_ACTIONS = Map.of(
        VERSION_ONE,
        Stream.of(
            SetPriorityAction.NAME,
            UnfollowAction.NAME,
            ReadOnlyAction.NAME,
            DownsampleAction.NAME,
            AllocateAction.NAME,
            MigrateAction.NAME,
            ShrinkAction.NAME,
            ForceMergeAction.NAME
        ).filter(Objects::nonNull).toList(),
        VERSION_TWO,
        Stream.of(
            SetPriorityAction.NAME,
            UnfollowAction.NAME,
            ReadOnlyAction.NAME,
            AllocateAction.NAME,
            MigrateAction.NAME,
            DownsampleAction.NAME,
            ShrinkAction.NAME,
            ForceMergeAction.NAME
        ).filter(Objects::nonNull).toList()
    );

    public static final Map<Integer, List<String>> ORDERED_VALID_COLD_ACTIONS = Map.of(
        VERSION_ONE,
        Stream.of(
            SetPriorityAction.NAME,
            UnfollowAction.NAME,
            ReadOnlyAction.NAME,
            DownsampleAction.NAME,
            SearchableSnapshotAction.NAME,
            AllocateAction.NAME,
            MigrateAction.NAME,
            FreezeAction.NAME
        ).filter(Objects::nonNull).toList(),
        VERSION_TWO,
        Stream.of(
            SetPriorityAction.NAME,
            UnfollowAction.NAME,
            ReadOnlyAction.NAME,
            SearchableSnapshotAction.NAME,
            AllocateAction.NAME,
            MigrateAction.NAME,
            DownsampleAction.NAME,
            FreezeAction.NAME
        ).filter(Objects::nonNull).toList()
    );

    public static final Map<Integer, List<String>> ORDERED_VALID_FROZEN_ACTIONS = Map.of(
        VERSION_ONE,
        List.of(UnfollowAction.NAME, SearchableSnapshotAction.NAME),
        VERSION_TWO,
        List.of(UnfollowAction.NAME, SearchableSnapshotAction.NAME)
    );
    public static final Map<Integer, List<String>> ORDERED_VALID_DELETE_ACTIONS = Map.of(
        VERSION_ONE,
        List.of(WaitForSnapshotAction.NAME, DeleteAction.NAME),
        VERSION_TWO,
        List.of(WaitForSnapshotAction.NAME, DeleteAction.NAME)
    );

    static final Set<String> VALID_HOT_ACTIONS = Sets.newHashSet(ORDERED_VALID_HOT_ACTIONS.get(CURRENT_VERSION));
    static final Set<String> VALID_WARM_ACTIONS = Sets.newHashSet(ORDERED_VALID_WARM_ACTIONS.get(CURRENT_VERSION));
    static final Set<String> VALID_COLD_ACTIONS = Sets.newHashSet(ORDERED_VALID_COLD_ACTIONS.get(CURRENT_VERSION));
    static final Set<String> VALID_FROZEN_ACTIONS = Sets.newHashSet(ORDERED_VALID_FROZEN_ACTIONS.get(CURRENT_VERSION));
    static final Set<String> VALID_DELETE_ACTIONS = Sets.newHashSet(ORDERED_VALID_DELETE_ACTIONS.get(CURRENT_VERSION));

    static final Map<String, Set<String>> ALLOWED_ACTIONS = Map.of(
        HOT_PHASE,
        VALID_HOT_ACTIONS,
        WARM_PHASE,
        VALID_WARM_ACTIONS,
        COLD_PHASE,
        VALID_COLD_ACTIONS,
        DELETE_PHASE,
        VALID_DELETE_ACTIONS,
        FROZEN_PHASE,
        VALID_FROZEN_ACTIONS
    );
}
