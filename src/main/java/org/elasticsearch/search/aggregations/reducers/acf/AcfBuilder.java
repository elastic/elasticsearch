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

package org.elasticsearch.search.aggregations.reducers.acf;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.reducers.ReducerBuilder;

import java.io.IOException;

import static org.elasticsearch.search.aggregations.reducers.BucketHelpers.GapPolicy;

public class AcfBuilder extends ReducerBuilder<AcfBuilder> {

    private String format;
    private GapPolicy gapPolicy;
    private Boolean zeroPad = true;
    private Boolean normalize = true;
    private Boolean zeroMean = true;
    private Integer window = 5;

    public AcfBuilder(String name) {
        super(name, AcfReducer.TYPE.name());
    }

    public AcfBuilder format(String format) {
        this.format = format;
        return this;
    }

    public AcfBuilder gapPolicy(GapPolicy gapPolicy) {
        this.gapPolicy = gapPolicy;
        return this;
    }

    public AcfBuilder settings(AcfSettings settings) {
        this.zeroPad = settings.isZeroPad();
        this.normalize = settings.isNormalize();
        this.zeroMean = settings.isZeroMean();
        this.window = settings.getWindow();
        return this;
    }

    public AcfBuilder zeroPad(boolean zeroPad) {
        this.zeroPad = zeroPad;
        return this;
    }

    public AcfBuilder normalize(boolean normalize) {
        this.normalize = normalize;
        return this;
    }

    public AcfBuilder zeroMean(boolean zeroMean) {
        this.zeroMean = zeroMean;
        return this;
    }

    public AcfBuilder window(int window) {
        this.window = window;
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (format != null) {
            builder.field(AcfParser.FORMAT.getPreferredName(), format);
        }
        if (gapPolicy != null) {
            builder.field(AcfParser.GAP_POLICY.getPreferredName(), gapPolicy.getName());
        }
        if (zeroPad != null) {
            builder.field(AcfParser.ZERO_PAD.getPreferredName(), zeroPad);
        }
        if (normalize != null) {
            builder.field(AcfParser.NORMALIZE.getPreferredName(), normalize);
        }
        if (zeroMean != null) {
            builder.field(AcfParser.ZERO_MEAN.getPreferredName(), zeroMean);
        }
        if (window != null) {
            builder.field(AcfParser.WINDOW.getPreferredName(), window);
        }
        return builder;
    }

}
