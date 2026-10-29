/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.columnar;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

/**
 * Upgrade tests turn this flag off, so the numeric format it gates never crosses a build. If this stops compiling or
 * fails, the flag has gone: delete the block in {@code DefaultSystemPropertyProvider} that sets
 * {@code es.columnar_codec_numeric_feature_flag_enabled} so those tests cover the format again.
 */
public class ColumnarCodecNumericUpgradeTestFlagTests extends ESTestCase {

    public void testTheFlagTheUpgradeTestsTurnOffIsStillTheOneThisCodecReads() {
        assertThat(
            ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_NUMERIC_FEATURE_FLAG.toString(),
            containsString("columnar_codec_numeric=")
        );
    }
}
