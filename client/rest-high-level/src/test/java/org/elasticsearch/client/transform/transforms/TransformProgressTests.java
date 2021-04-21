/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class TransformProgressTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
            TransformProgressTests::randomInstance,
            TransformProgressTests::toXContent,
            TransformProgress::fromXContent)
           .supportsUnknownFields(true)
           .test();
    }

    public static TransformProgress randomInstance() {
        return new TransformProgress(
            randomBoolean() ? null : randomNonNegativeLong(),
            randomBoolean() ? null : randomNonNegativeLong(),
            randomBoolean() ? null : randomDouble(),
            randomBoolean() ? null : randomNonNegativeLong(),
            randomBoolean() ? null : randomNonNegativeLong());
    }

    public static void toXContent(TransformProgress progress, XContentBuilder builder) throws IOException {
        builder.startObject();
        if (progress.getTotalDocs() != null) {
            builder.field(TransformProgress.TOTAL_DOCS.getPreferredName(), progress.getTotalDocs());
        }
        if (progress.getPercentComplete() != null) {
            builder.field(TransformProgress.PERCENT_COMPLETE.getPreferredName(), progress.getPercentComplete());
        }
        if (progress.getRemainingDocs() != null) {
            builder.field(TransformProgress.DOCS_REMAINING.getPreferredName(), progress.getRemainingDocs());
        }
        builder.field(TransformProgress.DOCS_INDEXED.getPreferredName(), progress.getDocumentsIndexed());
        builder.field(TransformProgress.DOCS_PROCESSED.getPreferredName(), progress.getDocumentsProcessed());
        builder.endObject();
    }
}
