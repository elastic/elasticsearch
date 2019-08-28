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

package org.elasticsearch.client.dataframe.transforms.hlrc;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformProgress;

import static org.hamcrest.Matchers.equalTo;

public class DataFrameTransformProgressTests extends AbstractResponseTestCase<
        DataFrameTransformProgress,
        org.elasticsearch.client.dataframe.transforms.DataFrameTransformProgress> {

    public static DataFrameTransformProgress fromHlrc(
            org.elasticsearch.client.dataframe.transforms.DataFrameTransformProgress instance) {
        if (instance == null) {
            return null;
        }
        return new DataFrameTransformProgress(instance.getTotalDocs(),
            instance.getRemainingDocs(),
            instance.getDocumentsProcessed(),
            instance.getDocumentsIndexed());
    }

    public static DataFrameTransformProgress randomDataFrameTransformProgress() {
        Long totalDocs = randomBoolean() ? null : randomNonNegativeLong();
        Long docsRemaining = totalDocs != null ? randomLongBetween(0, totalDocs) : null;
        return new DataFrameTransformProgress(
            totalDocs,
            docsRemaining,
            totalDocs != null ? totalDocs - docsRemaining : randomNonNegativeLong(),
            randomBoolean() ? null : randomNonNegativeLong());
    }

    @Override
    protected DataFrameTransformProgress createServerTestInstance(XContentType xContentType) {
        return randomDataFrameTransformProgress();
    }

    @Override
    protected org.elasticsearch.client.dataframe.transforms.DataFrameTransformProgress doParseToClientInstance(XContentParser parser) {
        return org.elasticsearch.client.dataframe.transforms.DataFrameTransformProgress.fromXContent(parser);
    }

    @Override
    protected void assertInstances(DataFrameTransformProgress serverTestInstance,
                                   org.elasticsearch.client.dataframe.transforms.DataFrameTransformProgress clientInstance) {
        assertThat(serverTestInstance.getTotalDocs(), equalTo(clientInstance.getTotalDocs()));
        assertThat(serverTestInstance.getDocumentsProcessed(), equalTo(clientInstance.getDocumentsProcessed()));
        assertThat(serverTestInstance.getPercentComplete(), equalTo(clientInstance.getPercentComplete()));
        assertThat(serverTestInstance.getDocumentsIndexed(), equalTo(clientInstance.getDocumentsIndexed()));
    }
}
