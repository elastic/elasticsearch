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

package org.elasticsearch.index.query.functionscore;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.lucene.search.function.WeightFactorFunction;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryValidationException;

import java.io.IOException;
import java.util.Objects;

public abstract class ScoreFunctionBuilder<FB extends ScoreFunctionBuilder> implements NamedWriteable<FB>, ToXContent {

    protected Float weight;

    public ScoreFunctionBuilder setWeight(float weight) {
        this.weight = weight;
        return this;
    }

    protected void buildWeight(XContentBuilder builder) throws IOException {
        if (weight != null) {
            builder.field(FunctionScoreQueryParser.WEIGHT_FIELD.getPreferredName(), weight);
        }
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        buildWeight(builder);
        doXContent(builder, params);
        return builder;
    }

    protected abstract void doXContent(XContentBuilder builder, Params params) throws IOException;

    protected ScoreFunction toScoreFunction(QueryShardContext context) throws IOException {
        // we need to take the weight into account
        ScoreFunction scoreFunction = doScoreFunction(context);
        if (scoreFunction instanceof WeightFactorFunction) {
            return scoreFunction;
        } else if (this.weight != null) {
            return new WeightFactorFunction(weight, scoreFunction);
        } else {
            return scoreFunction;
        }
    }

    protected abstract ScoreFunction doScoreFunction(QueryShardContext context) throws IOException;

    //NO COMMIT maybe have a FunctionScoreValidationException instead?
    public QueryValidationException validate() {
        // default impl does not validate, subclasses should override.
        //norelease to be possibly made abstract once all queries support validation
        return null;
    }

    @Override
    public final FB readFrom(StreamInput in) throws IOException {
        FB scoreFunctionBuilder = doReadFrom(in);
        scoreFunctionBuilder.weight = in.readOptionalFloat();
        return scoreFunctionBuilder;
    }

    protected abstract FB doReadFrom(StreamInput in) throws IOException;

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        doWriteTo(out);
        out.writeOptionalFloat(weight);
    }

    protected abstract void doWriteTo(StreamOutput out) throws IOException;

    protected final QueryValidationException addValidationError(String validationError, QueryValidationException validationException) {
        return QueryValidationException.addValidationError(getWriteableName(), validationError, validationException);
    }

    @Override
    public final int hashCode() {
        return 31 * Objects.hash(getClass(), weight) + doHashCode();
    }

    protected abstract int doHashCode();

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        @SuppressWarnings("unchecked")
        FB other = (FB) obj;
        return Objects.equals(weight, other.weight) &&
                doEquals(other);
    }

    protected abstract boolean doEquals(FB other);
}
