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

package org.elasticsearch.common.xcontent.support.filtering;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;

/**
 * Benchmark class to compare filtered and unfiltered XContent generators.
 */
public class FilteringJsonGeneratorBenchmark {

    public static void main(String[] args) throws IOException {
        final XContent XCONTENT = JsonXContent.jsonXContent;

        System.out.println("Executing " + FilteringJsonGeneratorBenchmark.class + "...");

        System.out.println("Warming up...");
        run(XCONTENT, 500_000, 100, 0.5);
        System.out.println("Warmed up.");

        System.out.println("nb documents | nb fields | nb fields written | % fields written | time (millis) | rate (docs/sec) | avg size");

        for (int nbFields : Arrays.asList(10, 25, 50, 100, 250)) {
            for (int nbDocs : Arrays.asList(100, 1000, 10_000, 100_000, 500_000)) {
                for (double ratio : Arrays.asList(0.0, 1.0, 0.99, 0.95, 0.9, 0.75, 0.5, 0.25, 0.1, 0.05, 0.01)) {
                    run(XCONTENT, nbDocs, nbFields, ratio);
                }
            }
        }
        System.out.println("Done.");
    }

    private static void run(XContent xContent, long nbIterations, int nbFields, double ratio) throws IOException {
        String[] fields = fields(nbFields);
        String[] filters = fields((int) (nbFields * ratio));

        long size = 0;
        BytesStreamOutput os = new BytesStreamOutput();

        long start = System.nanoTime();
        for (int i = 0; i < nbIterations; i++) {
            XContentBuilder builder = new XContentBuilder(xContent, os, filters);
            builder.startObject();

            for (String field : fields) {
                builder.field(field, System.nanoTime());
            }
            builder.endObject();

            size += builder.bytes().length();
            os.reset();
        }
        double milliseconds = (System.nanoTime() - start) / 1_000_000d;

        System.out.printf(Locale.ROOT, "%12d | %9d | %17d | %14.2f %% | %10.3f ms | %15.2f | %8.0f %n",
                nbIterations, nbFields,
                (int) (nbFields * ratio),
                (ratio * 100d),
                milliseconds,
                ((double) nbIterations) / (milliseconds / 1000d),
                size / ((double) nbIterations));
    }

    /**
     * Returns a String array of field names starting from "field_0" with a length of n.
     * If n=3, the array is ["field_0","field_1","field_2"]
     */
    private static String[] fields(int n) {
        String[] fields = new String[n];
        for (int i = 0; i < n; i++) {
            fields[i] = "field_" + i;
        }
        return fields;
    }
}
