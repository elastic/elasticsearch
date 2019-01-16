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
package org.elasticsearch.client.rollup;

import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class GetRollupCapsResponse {

    private final Map<String, RollableIndexCaps> jobs;

    public GetRollupCapsResponse(final Map<String, RollableIndexCaps> jobs) {
        this.jobs = Collections.unmodifiableMap(Objects.requireNonNull(jobs));
    }

    public Map<String, RollableIndexCaps> getJobs() {
        return jobs;
    }

    public static GetRollupCapsResponse fromXContent(final XContentParser parser) throws IOException {
        Map<String, RollableIndexCaps> jobs = new HashMap<>();
        XContentParser.Token token = parser.nextToken();
        if (token.equals(XContentParser.Token.START_OBJECT)) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token.equals(XContentParser.Token.FIELD_NAME)) {
                    String pattern = parser.currentName();

                    RollableIndexCaps cap = RollableIndexCaps.PARSER.apply(pattern).apply(parser, null);
                    jobs.put(pattern, cap);
                }
            }
        }
        return new GetRollupCapsResponse(jobs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobs);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        GetRollupCapsResponse other = (GetRollupCapsResponse) obj;
        return Objects.equals(jobs, other.jobs);
    }
}
