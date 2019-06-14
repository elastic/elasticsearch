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

package org.elasticsearch.client.dataframe.transforms;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DataFrameTransformProgress {

    public static final ParseField TOTAL_DOCS = new ParseField("total_docs");
    public static final ParseField DOCS_REMAINING = new ParseField("docs_remaining");
    public static final ParseField PERCENT_COMPLETE = new ParseField("percent_complete");

    public static final ConstructingObjectParser<DataFrameTransformProgress, Void> PARSER = new ConstructingObjectParser<>(
        "data_frame_transform_progress",
        true,
        a -> new DataFrameTransformProgress((Long) a[0], (Long)a[1], (Double)a[2]));

    static {
        PARSER.declareLong(constructorArg(), TOTAL_DOCS);
        PARSER.declareLong(optionalConstructorArg(), DOCS_REMAINING);
        PARSER.declareDouble(optionalConstructorArg(), PERCENT_COMPLETE);
    }

    public static DataFrameTransformProgress fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final long totalDocs;
    private final long remainingDocs;
    private final double percentComplete;

    public DataFrameTransformProgress(long totalDocs, Long remainingDocs, double percentComplete) {
        this.totalDocs = totalDocs;
        this.remainingDocs = remainingDocs == null ? totalDocs : remainingDocs;
        this.percentComplete = percentComplete;
    }

    public double getPercentComplete() {
        return percentComplete;
    }

    public long getTotalDocs() {
        return totalDocs;
    }

    public long getRemainingDocs() {
        return remainingDocs;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (other == null || other.getClass() != getClass()) {
            return false;
        }

        DataFrameTransformProgress that = (DataFrameTransformProgress) other;
        return Objects.equals(this.remainingDocs, that.remainingDocs)
            && Objects.equals(this.totalDocs, that.totalDocs)
            && Objects.equals(this.percentComplete, that.percentComplete);
    }

    @Override
    public int hashCode(){
        return Objects.hash(remainingDocs, totalDocs, percentComplete);
    }
}
