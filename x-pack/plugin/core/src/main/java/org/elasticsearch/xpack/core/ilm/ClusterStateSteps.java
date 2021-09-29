/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import java.util.Set;

import static org.elasticsearch.xpack.core.ilm.ForceMergeAction.CONDITIONAL_SKIP_FORCE_MERGE_STEP;
import static org.elasticsearch.xpack.core.ilm.FreezeAction.CONDITIONAL_SKIP_FREEZE_STEP;
import static org.elasticsearch.xpack.core.ilm.MigrateAction.CONDITIONAL_SKIP_MIGRATE_STEP;
import static org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction.CONDITIONAL_DATASTREAM_CHECK_KEY;
import static org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction.CONDITIONAL_SKIP_ACTION_STEP;
import static org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction.CONDITIONAL_SKIP_GENERATE_AND_CLEAN;
import static org.elasticsearch.xpack.core.ilm.UnfollowAction.CONDITIONAL_UNFOLLOW_STEP;

public class ClusterStateSteps {

    public static final Set<String> NAMES = Set.of(BranchingStep.NAME, CopyExecutionStateStep.NAME, CopySettingsStep.NAME,
        GenerateSnapshotNameStep.NAME, GenerateUniqueIndexNameStep.NAME, InitializePolicyContextStep.NAME,
        ReplaceDataStreamBackingIndexStep.NAME, UpdateRolloverLifecycleDateStep.NAME, AllocationRoutedStep.NAME,
        CheckNotDataStreamWriteIndexStep.NAME, CheckShrinkReadyStep.NAME, CheckTargetShardsCountStep.NAME,
        DataTierMigrationRoutedStep.NAME, ShrunkShardsAllocatedStep.NAME, ShrunkenIndexCheckStep.NAME, WaitForActiveShardsStep.NAME,
        WaitForDataTierStep.NAME, WaitForIndexColorStep.NAME, WaitForIndexingCompleteStep.NAME, WaitForSnapshotStep.NAME,
        PhaseCompleteStep.NAME, CONDITIONAL_UNFOLLOW_STEP, CONDITIONAL_SKIP_FORCE_MERGE_STEP, CONDITIONAL_SKIP_FREEZE_STEP,
        CONDITIONAL_SKIP_MIGRATE_STEP, CONDITIONAL_SKIP_ACTION_STEP, CONDITIONAL_SKIP_GENERATE_AND_CLEAN, CONDITIONAL_DATASTREAM_CHECK_KEY);
}
