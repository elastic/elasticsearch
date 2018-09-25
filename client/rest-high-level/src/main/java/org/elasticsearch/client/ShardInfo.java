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

package org.elasticsearch.client;

import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Encapsulates the number of total successful and failed shard copies
 */
public final class ShardInfo {

    public static final String SHARDS_FIELD = "_shards";
    private static final String TOTAL_FIELD = "total";
    private static final String SUCCESSFUL_FIELD = "successful";
    private static final String FAILED_FIELD = "failed";
    private static final String FAILURES = "failures";

    static final ConstructingObjectParser<ShardInfo, Void> PARSER =
        new ConstructingObjectParser<>(
            "_shards",
            a -> {
                List<ReplicationResponse.ShardInfo.Failure> failures;
                if (a[3] == null) {
                    failures = Collections.emptyList();
                } else {
                    failures = Collections.unmodifiableList(Arrays.asList((ReplicationResponse.ShardInfo.Failure[]) a[3]));
                }
                return new ShardInfo((Integer) a[0], (Integer) a[1], (Integer) a[2], failures);
            }
        );
    static {
        PARSER.declareInt(constructorArg(), new ParseField(TOTAL_FIELD));
        PARSER.declareInt(constructorArg(), new ParseField(SUCCESSFUL_FIELD));
        PARSER.declareInt(constructorArg(), new ParseField(FAILED_FIELD));
        PARSER.declareObjectArray(optionalConstructorArg(),
                (p, c) -> ReplicationResponse.ShardInfo.Failure.fromXContent(p), new ParseField(FAILURES));
    }

    private final int total;
    private final int successful;
    private final int failed;
    private final List<ReplicationResponse.ShardInfo.Failure> failures; // TODO: don't reuse server classes

    private ShardInfo(int total, int successful, int failed, List<ReplicationResponse.ShardInfo.Failure> failures) {
        this.total = total;
        this.successful = successful;
        this.failed = failed;
        this.failures = failures;
    }

    /**
     * Return the total number of shards.
     */
    public int getTotal() {
        return total;
    }

    /**
     * Return the number of successful shards.
     */
    public int getSuccessful() {
        return successful;
    }

    /**
     * Return the number of failed shards.
     */
    public int getFailed() {
        return failed;
    }

    /**
     * Return shard failures.
     */
    public List<ReplicationResponse.ShardInfo.Failure> getFailures() {
        return failures;
    }

    @Override
    public int hashCode() {
        int h = failed;
        h = 31 * h + successful;
        h = 31 * h + total;
        h = 31 * h + failures.hashCode();
        return h;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ShardInfo that = (ShardInfo) obj;
        return failed == that.failed &&
                successful == that.successful &&
                total == that.total &&
                failures.equals(that.failures);
    }

}
