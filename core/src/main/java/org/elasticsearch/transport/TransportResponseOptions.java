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

package org.elasticsearch.transport;

public class TransportResponseOptions {

    private final boolean compress;

    private TransportResponseOptions(boolean compress) {
        this.compress = compress;
    }

    public boolean compress() {
        return this.compress;
    }

    public static final TransportResponseOptions EMPTY = TransportResponseOptions.builder().build();

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(TransportResponseOptions options) {
        return new Builder()
                .withCompress(options.compress);
    }

    public static class Builder {
        private boolean compress;

        public Builder withCompress(boolean compress) {
            this.compress = compress;
            return this;
        }

        public TransportResponseOptions build() {
            return new TransportResponseOptions(compress);
        }
    }
}
