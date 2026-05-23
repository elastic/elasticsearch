/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class BYOSemanticHandlerTests extends ESTestCase {

    public void testParseStageInit() {
        assertThat(BYOSemanticAction.fromString("stage_init"), equalTo(BYOSemanticAction.STAGE_INIT));
        assertThat(BYOSemanticAction.fromString("STAGE_INIT"), equalTo(BYOSemanticAction.STAGE_INIT));
        assertThat(BYOSemanticAction.fromString("Stage_Init"), equalTo(BYOSemanticAction.STAGE_INIT));
    }

    public void testParseStage() {
        assertThat(BYOSemanticAction.fromString("stage"), equalTo(BYOSemanticAction.STAGE));
        assertThat(BYOSemanticAction.fromString("STAGE"), equalTo(BYOSemanticAction.STAGE));
        assertThat(BYOSemanticAction.fromString("Stage"), equalTo(BYOSemanticAction.STAGE));
    }

    public void testParseCommit() {
        assertThat(BYOSemanticAction.fromString("commit"), equalTo(BYOSemanticAction.COMMIT));
        assertThat(BYOSemanticAction.fromString("COMMIT"), equalTo(BYOSemanticAction.COMMIT));
        assertThat(BYOSemanticAction.fromString("Commit"), equalTo(BYOSemanticAction.COMMIT));
    }

    public void testParseCancel() {
        assertThat(BYOSemanticAction.fromString("cancel"), equalTo(BYOSemanticAction.CANCEL));
        assertThat(BYOSemanticAction.fromString("CANCEL"), equalTo(BYOSemanticAction.CANCEL));
        assertThat(BYOSemanticAction.fromString("Cancel"), equalTo(BYOSemanticAction.CANCEL));
    }

    public void testParseUnknownActionThrows() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> BYOSemanticAction.fromString("unknown_action"));
        assertThat(ex.getMessage(), equalTo("Unknown BYO semantic action [unknown_action]"));
    }

    public void testParseNullActionThrows() {
        expectThrows(NullPointerException.class, () -> BYOSemanticAction.fromString(null));
    }
}
