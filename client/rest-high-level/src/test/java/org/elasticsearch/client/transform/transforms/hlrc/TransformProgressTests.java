/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
