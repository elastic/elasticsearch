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

package org.elasticsearch.client.core;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

public class BroadcastResponse {

    private final Shards shards;

    public Shards shards() {
        return shards;
    }

    BroadcastResponse(final Shards shards) {
        this.shards = Objects.requireNonNull(shards);
    }

    private static final ParseField SHARDS_FIELD = new ParseField("_shards");

    static final ConstructingObjectParser<BroadcastResponse, Void> PARSER = new ConstructingObjectParser<>(
            "broadcast_response",
            a -> new BroadcastResponse((Shards) a[0]));

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), Shards.SHARDS_PARSER, SHARDS_FIELD);
    }

    public static BroadcastResponse fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static class Shards {

        private final int total;

        public int total() {
            return total;
        }

        private final int successful;

        public int successful() {
            return successful;
        }

        private final int skipped;

        public int skipped() {
            return skipped;
        }

        private final int failed;

        public int failed() {
            return failed;
        }

        private final Collection<DefaultShardOperationFailedException> failures;

        public Collection<DefaultShardOperationFailedException> failures() {
            return failures;
        }

        Shards(
                final int total,
                final int successful,
                final int skipped,
                final int failed,
                final Collection<DefaultShardOperationFailedException> failures) {
            this.total = total;
            this.successful = successful;
            this.skipped = skipped;
            this.failed = failed;
            this.failures = Collections.unmodifiableCollection(Objects.requireNonNull(failures));
        }

        private static final ParseField TOTAL_FIELD = new ParseField("total");
        private static final ParseField SUCCESSFUL_FIELD = new ParseField("successful");
        private static final ParseField SKIPPED_FIELD = new ParseField("skipped");
        private static final ParseField FAILED_FIELD = new ParseField("failed");
        private static final ParseField FAILURES_FIELD = new ParseField("failures");

        @SuppressWarnings("unchecked")
        static final ConstructingObjectParser<Shards, Void> SHARDS_PARSER = new ConstructingObjectParser<>(
                "shards",
                a -> new Shards(
                        (int) a[0], // total
                        (int) a[1], // successful
                        a[2] == null ? 0 : (int) a[2], // skipped
                        (int) a[3], // failed
                        a[4] == null ? Collections.emptyList() : (Collection<DefaultShardOperationFailedException>) a[4])); // failures

        static {
            SHARDS_PARSER.declareInt(ConstructingObjectParser.constructorArg(), TOTAL_FIELD);
            SHARDS_PARSER.declareInt(ConstructingObjectParser.constructorArg(), SUCCESSFUL_FIELD);
            SHARDS_PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), SKIPPED_FIELD);
            SHARDS_PARSER.declareInt(ConstructingObjectParser.constructorArg(), FAILED_FIELD);
            SHARDS_PARSER.declareObjectArray(
                    ConstructingObjectParser.optionalConstructorArg(),
                    DefaultShardOperationFailedException.PARSER, FAILURES_FIELD);
        }

    }

}
