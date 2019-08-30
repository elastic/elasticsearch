/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StoredProgressTests extends AbstractXContentTestCase<StoredProgress> {

    @Override
    protected StoredProgress doParseInstance(XContentParser parser) throws IOException {
        return StoredProgress.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected StoredProgress createTestInstance() {
        int phaseCount = randomIntBetween(3, 7);
        List<PhaseProgress> progress = new ArrayList<>(phaseCount);
        for (int i = 0; i < phaseCount; i++) {
            progress.add(new PhaseProgress(randomAlphaOfLength(10), randomIntBetween(0, 100)));
        }
        return new StoredProgress(progress);
    }
}
