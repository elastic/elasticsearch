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

package org.elasticsearch.search.profile;

/**
 * Public interface used to consume profiled time breakdowns per query node.
 * A node's time may be composed of several internal attributes (rewriting, weighting,
 * scoring, etc).
 */
public interface ProfileBreakdown {

    enum TimingType {
        REWRITE(0), WEIGHT(1), SCORE(2), COST(3), NORMALIZE(4), BUILD_SCORER(5);

        private int type;

        TimingType(int type) {
            this.type = type;
        }

        public int getType() {
            return type;
        }

        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }

    long getTime(TimingType type);
}
