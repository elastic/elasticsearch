/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.elasticsearch.test.ESTestCase;

public class StageSpecTests extends ESTestCase {

    public void testAllStageSpecsHaveStageIds() {
        assertNotNull(new StageSpec.DeltaStage().stageId());
        assertNotNull(new StageSpec.OffsetStage().stageId());
        assertNotNull(new StageSpec.GcdStage().stageId());
        assertNotNull(new StageSpec.BitPackPayload().stageId());
    }
}
