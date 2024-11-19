/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.client.benchmark;

import org.elasticsearch.client.benchmark.rest.RestClientBenchmark;
import org.elasticsearch.core.SuppressForbidden;

import java.util.Arrays;

public class BenchmarkMain {
    @SuppressForbidden(reason = "system out is ok for a command line tool")
    public static void main(String[] args) throws Exception {
        String type = args[0];
        AbstractBenchmark<?> benchmark = switch (type) {
            case "rest" -> new RestClientBenchmark();
            default -> {
                System.err.println("Unknown client type [" + type + "]");
                System.exit(1);
                yield null;
            }
        };
        benchmark.run(Arrays.copyOfRange(args, 1, args.length));
    }
}
