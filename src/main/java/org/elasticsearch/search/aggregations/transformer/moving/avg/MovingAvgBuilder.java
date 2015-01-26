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

package org.elasticsearch.search.aggregations.transformer.moving.avg;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.transformer.derivative.Derivative.GapPolicy;

import java.io.IOException;

public class MovingAvgBuilder extends ValuesSourceAggregationBuilder<MovingAvgBuilder> {

    private GapPolicy gapPolicy;
    private String format;
    private Boolean keyed;
    private MovingAvg.Weighting weight;
    private Integer windowSize;

    public MovingAvgBuilder(String name) {
        super(name, InternalMovingAvg.TYPE.name());
    }

    public void gapPolicy(GapPolicy gapPolicy) {
        this.gapPolicy = gapPolicy;
    }

    public void format(String format) {
        this.format = format;
    }

    public void keyed(boolean keyed) {
        this.keyed = keyed;
    }

    public void weight(MovingAvg.Weighting weight) {
        this.weight = weight;
    }

    public void windowSize(int windowSize) {
        this.windowSize = windowSize;
    }

    @Override
    protected XContentBuilder doInternalXContent(XContentBuilder builder, Params params) throws IOException {
        if (gapPolicy != null) {
            builder.field(MovingAvgParser.GAP_POLICY.getPreferredName(), gapPolicy.name());
        }
        if (format != null) {
            builder.field(MovingAvgParser.FORMAT.getPreferredName(), format);
        }
        if (keyed != null) {
            builder.field(MovingAvgParser.KEYED.getPreferredName(), keyed);
        }
        if (weight != null) {
            builder.field(MovingAvgParser.WEIGHTING.getPreferredName(), weight.name());
        }
        if (windowSize != null) {
            builder.field(MovingAvgParser.WINDOW.getPreferredName(), windowSize);
        }
        return builder;
    }

}
