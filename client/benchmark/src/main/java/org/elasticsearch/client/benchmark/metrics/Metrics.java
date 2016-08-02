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
package org.elasticsearch.client.benchmark.metrics;

public final class Metrics {
    public final String operation;
    public final long successCount;
    public final long errorCount;
    public final double throughput;
    public final double serviceTimeP90;
    public final double serviceTimeP95;
    public final double serviceTimeP99;
    public final double serviceTimeP999;
    public final double serviceTimeP9999;

    public Metrics(String operation, long successCount, long errorCount, double throughput,
                   double serviceTimeP90, double serviceTimeP95, double serviceTimeP99,
                   double serviceTimeP999, double serviceTimeP9999) {
        this.operation = operation;
        this.successCount = successCount;
        this.errorCount = errorCount;
        this.throughput = throughput;
        this.serviceTimeP90 = serviceTimeP90;
        this.serviceTimeP95 = serviceTimeP95;
        this.serviceTimeP99 = serviceTimeP99;
        this.serviceTimeP999 = serviceTimeP999;
        this.serviceTimeP9999 = serviceTimeP9999;
    }
}
