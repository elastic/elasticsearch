/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.dataframe.transforms;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class DataFrameTransformProgressTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
            DataFrameTransformProgressTests::randomInstance,
            DataFrameTransformProgressTests::toXContent,
            DataFrameTransformProgress::fromXContent)
           .supportsUnknownFields(true)
           .randomFieldsExcludeFilter(field -> field.startsWith("state"))
           .test();
    }

    public static DataFrameTransformProgress randomInstance() {
        long totalDocs = randomNonNegativeLong();
        Long docsRemaining = randomBoolean() ? null : randomLongBetween(0, totalDocs);
        double percentComplete = totalDocs == 0 ? 1.0 : docsRemaining == null ? 0.0 : 100.0*(double)(totalDocs - docsRemaining)/totalDocs;
        return new DataFrameTransformProgress(totalDocs, docsRemaining, percentComplete);
    }

    public static void toXContent(DataFrameTransformProgress progress, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field(DataFrameTransformProgress.TOTAL_DOCS.getPreferredName(), progress.getTotalDocs());
        builder.field(DataFrameTransformProgress.DOCS_REMAINING.getPreferredName(), progress.getRemainingDocs());
        builder.field(DataFrameTransformProgress.PERCENT_COMPLETE.getPreferredName(), progress.getPercentComplete());
        builder.endObject();
    }
}
