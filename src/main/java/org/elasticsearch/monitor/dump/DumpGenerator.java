/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.monitor.dump;


import org.elasticsearch.common.Nullable;

import java.io.File;
import java.util.Map;

/**
 *
 */
public interface DumpGenerator {

    Result generateDump(String cause, @Nullable Map<String, Object> context) throws DumpGenerationFailedException;

    Result generateDump(String cause, @Nullable Map<String, Object> context, String... contributors) throws DumpGenerationFailedException;

    static class Result {
        private final File location;
        private Iterable<DumpContributionFailedException> failedContributors;

        public Result(File location, Iterable<DumpContributionFailedException> failedContributors) {
            this.location = location;
            this.failedContributors = failedContributors;
        }

        public String location() {
            return location.toString();
        }

        public Iterable<DumpContributionFailedException> failedContributors() {
            return failedContributors;
        }
    }
}
