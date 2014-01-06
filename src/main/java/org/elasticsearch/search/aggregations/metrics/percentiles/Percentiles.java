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
import org.elasticsearch.search.aggregations.Aggregation;

import java.io.IOException;

/**
 *
 */
public interface Percentiles extends Aggregation, Iterable<Percentiles.Percentile> {

    public static abstract class Estimator {

        public static TDigest tDigest() {
            return new TDigest();
        }

        private final String type;

        protected Estimator(String type) {
            this.type = type;
        }

        public static class TDigest extends Estimator {

            protected double compression = -1;

            TDigest() {
                super("tdigest");
            }

            public TDigest compression(double compression) {
                this.compression = compression;
                return this;
            }

            @Override
            void paramsToXContent(XContentBuilder builder) throws IOException {
                if (compression > 0) {
                    builder.field("compression", compression);
                }
            }
        }

        String type() {
            return type;
        }

        abstract void paramsToXContent(XContentBuilder builder) throws IOException;

    }

    public static interface Percentile {

        double getPercent();

        double getValue();

    }

    double percentile(double percent);

}
