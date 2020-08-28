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
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Analysis limits for autodetect. In particular,
 * this is a collection of parameters that allow limiting
 * the resources used by the job.
 */
public class AnalysisLimits implements ToXContentObject {

    /**
     * Serialisation field names
     */
    public static final ParseField MODEL_MEMORY_LIMIT = new ParseField("model_memory_limit");
    public static final ParseField CATEGORIZATION_EXAMPLES_LIMIT = new ParseField("categorization_examples_limit");

    public static final ConstructingObjectParser<AnalysisLimits, Void> PARSER =
        new ConstructingObjectParser<>("analysis_limits", true, a -> new AnalysisLimits((Long) a[0], (Long) a[1]));

    static {
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return ByteSizeValue.parseBytesSizeValue(p.text(), MODEL_MEMORY_LIMIT.getPreferredName()).getMb();
            } else if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                return p.longValue();
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, MODEL_MEMORY_LIMIT, ObjectParser.ValueType.VALUE);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), CATEGORIZATION_EXAMPLES_LIMIT);
    }

    /**
     * The model memory limit in MiBs.
     * It is initialised to <code>null</code>, which implies that the server-side default will be used.
     */
    private final Long modelMemoryLimit;

    /**
     * It is initialised to <code>null</code>.
     * A value of <code>null</code> will result in the server-side default being used.
     */
    private final Long categorizationExamplesLimit;

    public AnalysisLimits(Long categorizationExamplesLimit) {
        this(null, categorizationExamplesLimit);
    }

    public AnalysisLimits(Long modelMemoryLimit, Long categorizationExamplesLimit) {
        this.modelMemoryLimit = modelMemoryLimit;
        this.categorizationExamplesLimit = categorizationExamplesLimit;
    }

    /**
     * Maximum size of the model in MB before the anomaly detector
     * will drop new samples to prevent the model using any more
     * memory.
     *
     * @return The set memory limit or <code>null</code> if not set
     */
    @Nullable
    public Long getModelMemoryLimit() {
        return modelMemoryLimit;
    }

    /**
     * Gets the limit to the number of examples that are stored per category
     *
     * @return the limit or <code>null</code> if not set
     */
    @Nullable
    public Long getCategorizationExamplesLimit() {
        return categorizationExamplesLimit;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (modelMemoryLimit != null) {
            builder.field(MODEL_MEMORY_LIMIT.getPreferredName(), modelMemoryLimit + "mb");
        }
        if (categorizationExamplesLimit != null) {
            builder.field(CATEGORIZATION_EXAMPLES_LIMIT.getPreferredName(), categorizationExamplesLimit);
        }
        builder.endObject();
        return builder;
    }

    /**
     * Overridden equality test
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof AnalysisLimits == false) {
            return false;
        }

        AnalysisLimits that = (AnalysisLimits) other;
        return Objects.equals(this.modelMemoryLimit, that.modelMemoryLimit) &&
                Objects.equals(this.categorizationExamplesLimit, that.categorizationExamplesLimit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelMemoryLimit, categorizationExamplesLimit);
    }
}
