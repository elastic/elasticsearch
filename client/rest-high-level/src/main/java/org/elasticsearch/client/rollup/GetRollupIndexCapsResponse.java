/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.rollup;

import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class GetRollupIndexCapsResponse {

    private final Map<String, RollableIndexCaps> jobs;

    public GetRollupIndexCapsResponse(final Map<String, RollableIndexCaps> jobs) {
        this.jobs = Collections.unmodifiableMap(Objects.requireNonNull(jobs));
    }

    public Map<String, RollableIndexCaps> getJobs() {
        return jobs;
    }

    public static GetRollupIndexCapsResponse fromXContent(final XContentParser parser) throws IOException {
        Map<String, RollableIndexCaps> jobs = new HashMap<>();
        XContentParser.Token token = parser.nextToken();
        if (token.equals(XContentParser.Token.START_OBJECT)) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token.equals(XContentParser.Token.FIELD_NAME)) {
                    String pattern = parser.currentName();

                    RollableIndexCaps cap = RollableIndexCaps.PARSER.apply(parser, pattern);
                    jobs.put(pattern, cap);
                }
            }
        }
        return new GetRollupIndexCapsResponse(jobs);
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
        GetRollupIndexCapsResponse other = (GetRollupIndexCapsResponse) obj;
        return Objects.equals(jobs, other.jobs);
    }
}
