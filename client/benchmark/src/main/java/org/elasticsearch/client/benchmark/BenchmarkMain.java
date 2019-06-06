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
package org.elasticsearch.client.benchmark;

import org.elasticsearch.client.benchmark.rest.RestClientBenchmark;
import org.elasticsearch.common.SuppressForbidden;

import java.util.Arrays;

public class BenchmarkMain {
    @SuppressForbidden(reason = "system out is ok for a command line tool")
    public static void main(String[] args) throws Exception {
        String type = args[0];
        AbstractBenchmark<?> benchmark = null;
        switch (type) {
            case "rest":
                benchmark = new RestClientBenchmark();
                break;
            default:
                System.err.println("Unknown client type [" + type + "]");
                System.exit(1);
        }
        benchmark.run(Arrays.copyOfRange(args, 1, args.length));
    }
}
