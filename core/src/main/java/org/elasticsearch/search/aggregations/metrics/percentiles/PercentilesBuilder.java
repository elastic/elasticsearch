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
package org.elasticsearch.search.aggregations.metrics.percentiles;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;


/**
 * Builder for the {@link Percentiles} aggregation.
 */
public class PercentilesBuilder extends AbstractPercentilesBuilder<PercentilesBuilder> {

    double[] percentiles;
    /**
     * Sole constructor.
     */
    public PercentilesBuilder(String name) {
        super(name, Percentiles.TYPE_NAME);
    }

    /**
     * Set the percentiles to compute.
     */
    public PercentilesBuilder percentiles(double... percentiles) {
        for (int i = 0; i < percentiles.length; i++) {
            if (percentiles[i] < 0 || percentiles[i] > 100) {
                throw new IllegalArgumentException("the percents in the percentiles aggregation [" +
                        getName() + "] must be in the [0, 100] range");
            }
        }
        this.percentiles = percentiles;
        return this;
    }

    @Override
    protected void doInternalXContent(XContentBuilder builder, Params params) throws IOException {
        if (percentiles != null) {
            builder.field(PercentilesParser.PERCENTS_FIELD.getPreferredName(), percentiles);
        }
    }

}
