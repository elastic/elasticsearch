/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms.hlrc;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;

import static org.hamcrest.Matchers.equalTo;

public class TransformProgressTests extends AbstractResponseTestCase<
        TransformProgress,
        org.elasticsearch.client.transform.transforms.TransformProgress> {

    public static TransformProgress fromHlrc(
            org.elasticsearch.client.transform.transforms.TransformProgress instance) {
        if (instance == null) {
            return null;
        }
        return new TransformProgress(instance.getTotalDocs(),
            instance.getRemainingDocs(),
            instance.getDocumentsProcessed(),
            instance.getDocumentsIndexed());
    }

    public static TransformProgress randomTransformProgress() {
        Long totalDocs = randomBoolean() ? null : randomNonNegativeLong();
        Long docsRemaining = totalDocs != null ? randomLongBetween(0, totalDocs) : null;
        return new TransformProgress(
            totalDocs,
            docsRemaining,
            totalDocs != null ? totalDocs - docsRemaining : randomNonNegativeLong(),
            randomBoolean() ? null : randomNonNegativeLong());
    }

    @Override
    protected TransformProgress createServerTestInstance(XContentType xContentType) {
        return randomTransformProgress();
    }

    @Override
    protected org.elasticsearch.client.transform.transforms.TransformProgress doParseToClientInstance(XContentParser parser) {
        return org.elasticsearch.client.transform.transforms.TransformProgress.fromXContent(parser);
    }

    @Override
    protected void assertInstances(TransformProgress serverTestInstance,
                                   org.elasticsearch.client.transform.transforms.TransformProgress clientInstance) {
        assertThat(serverTestInstance.getTotalDocs(), equalTo(clientInstance.getTotalDocs()));
        assertThat(serverTestInstance.getDocumentsProcessed(), equalTo(clientInstance.getDocumentsProcessed()));
        assertThat(serverTestInstance.getPercentComplete(), equalTo(clientInstance.getPercentComplete()));
        assertThat(serverTestInstance.getDocumentsIndexed(), equalTo(clientInstance.getDocumentsIndexed()));
    }
}
