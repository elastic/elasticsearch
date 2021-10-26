/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.search.fetch.subphase;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.filtering.FilterNode;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author weizijun.wzj
 * @date 2021/10/26
 */
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class OldFilterContentBenchmark {
    @Param({ "ben1", "ben2", "ben3" })
    private String type;
    private FilterNode[] excludesFilters;
    private BytesReference inputSource;

    private static final String SOURCE_2 = "{\n"
        + "\t\"geonameid\": 110750,\n"
        + "\t\"name\": \"Abū al Khirrān\",\n"
        + "\t\"asciiname\": \"Abu al Khirran\",\n"
        + "\t\"alternatenames\": \"Aba al Khirran,Abu al Khirran,Abā al Khirrān,Abū al Khirrān\",\n"
        + "\t\"feature_class\": \"H\",\n"
        + "\t\"feature_code\": \"WLL\",\n"
        + "\t\"country_code\": \"SA\",\n"
        + "\t\"admin1_code\": \"06\",\n"
        + "\t\"population\": 0,\n"
        + "\t\"dem\": \"350\",\n"
        + "\t\"timezone\": \"Asia/Riyadh\""
        + "}";

    private static final String SOURCE_3 = "{\n" +
        "\t\"geonameid\": 1896836,\n" +
        "\t\"name\": \"Suryeul\",\n" +
        "\t\"asciiname\": \"Suryeul\",\n" +
        "\t\"alternatenames\": \"Sureul,Suryeul,sule-ul,sulyeul,수레울,수례울\",\n" +
        "\t\"feature_class\": \"P\",\n" +
        "\t\"feature_code\": \"PPL\",\n" +
        "\t\"country_code\": \"KR\",\n" +
        "\t\"admin1_code\": \"05\",\n" +
        "\t\"population\": 0,\n" +
        "\t\"dem\": \"203\",\n" +
        "\t\"timezone\": \"Asia/Seoul\",\n" +
        "\t\"location\": [127.64708, 37.04518]\n" +
        "}";
    private static final String SOURCE_4 = "{\"geonameid\": 5536939, \"name\": \"Chynoweth Canyon\", \"asciiname\": \"Chynoweth Canyon\", \"feature_class\": \"T\", \"feature_code\": \"VAL\", \"country_code\": \"US\", \"admin1_code\": \"UT\", \"admin2_code\": \"025\", \"population\": 0, \"elevation\": 1421, \"dem\": \"1421\", \"timezone\": \"America/Denver\", \"location\": [-111.76184, 37.2461]}";
    private static final Set<String> dvFields = Sets.newHashSet("geonameid", "population", "elevation", "location");

    String monSource = "{\n"
        + "          \"cluster_uuid\" : \"R0aKM8oRQs2Wq_x9y1O0Uw\",\n"
        + "          \"timestamp\" : \"2021-10-19T00:00:16.651Z\",\n"
        + "          \"interval_ms\" : 10000,\n"
        + "          \"type\" : \"index_stats\",\n"
        + "          \"source_node\" : {\n"
        + "            \"uuid\" : \"bBD4if6GRge8C27GEkyGoQ\",\n"
        + "            \"host\" : \"172.31.243.77\",\n"
        + "            \"transport_address\" : \"172.31.243.77:9300\",\n"
        + "            \"ip\" : \"172.31.243.77\",\n"
        + "            \"name\" : \"es-cn-2r42bit3z004thbby-94f438bf-0002\",\n"
        + "            \"timestamp\" : \"2021-10-19T00:00:16.519Z\"\n"
        + "          },\n"
        + "          \"index_stats\" : {\n"
        + "            \"index\" : \".ds-es_monitor_1m-2021.10.16-000054\",\n"
        + "            \"uuid\" : \"tsoVAyBFT22mdCFDTSkhPg\",\n"
        + "            \"created\" : 1634398585805,\n"
        + "            \"status\" : \"green\",\n"
        + "            \"shards\" : {\n"
        + "              \"total\" : 25,\n"
        + "              \"primaries\" : 25,\n"
        + "              \"replicas\" : 0,\n"
        + "              \"active_total\" : 25,\n"
        + "              \"active_primaries\" : 25,\n"
        + "              \"active_replicas\" : 0,\n"
        + "              \"unassigned_total\" : 0,\n"
        + "              \"unassigned_primaries\" : 0,\n"
        + "              \"unassigned_replicas\" : 0,\n"
        + "              \"initializing\" : 0,\n"
        + "              \"relocating\" : 0\n"
        + "            },\n"
        + "            \"total\" : {\n"
        + "              \"docs\" : {\n"
        + "                \"count\" : 1414978194\n"
        + "              },\n"
        + "              \"store\" : {\n"
        + "                \"size_in_bytes\" : 615609253702\n"
        + "              },\n"
        + "              \"indexing\" : {\n"
        + "                \"index_total\" : 0,\n"
        + "                \"index_time_in_millis\" : 0,\n"
        + "                \"throttle_time_in_millis\" : 0\n"
        + "              },\n"
        + "              \"search\" : {\n"
        + "                \"query_total\" : 5095,\n"
        + "                \"query_time_in_millis\" : 233865\n"
        + "              },\n"
        + "              \"merges\" : {\n"
        + "                \"total_size_in_bytes\" : 894157947088\n"
        + "              },\n"
        + "              \"refresh\" : {\n"
        + "                \"total_time_in_millis\" : 3329,\n"
        + "                \"external_total_time_in_millis\" : 29369\n"
        + "              },\n"
        + "              \"query_cache\" : {\n"
        + "                \"memory_size_in_bytes\" : 76891761,\n"
        + "                \"hit_count\" : 267,\n"
        + "                \"miss_count\" : 12907,\n"
        + "                \"evictions\" : 104\n"
        + "              },\n"
        + "              \"fielddata\" : {\n"
        + "                \"memory_size_in_bytes\" : 26784,\n"
        + "                \"evictions\" : 0\n"
        + "              },\n"
        + "              \"segments\" : {\n"
        + "                \"count\" : 125,\n"
        + "                \"memory_in_bytes\" : 60176756,\n"
        + "                \"terms_memory_in_bytes\" : 7794688,\n"
        + "                \"stored_fields_memory_in_bytes\" : 155352,\n"
        + "                \"term_vectors_memory_in_bytes\" : 0,\n"
        + "                \"norms_memory_in_bytes\" : 0,\n"
        + "                \"points_memory_in_bytes\" : 0,\n"
        + "                \"doc_values_memory_in_bytes\" : 52226716,\n"
        + "                \"index_writer_memory_in_bytes\" : 0,\n"
        + "                \"version_map_memory_in_bytes\" : 0,\n"
        + "                \"fixed_bit_set_memory_in_bytes\" : 0\n"
        + "              },\n"
        + "              \"request_cache\" : {\n"
        + "                \"memory_size_in_bytes\" : 131155,\n"
        + "                \"evictions\" : 0,\n"
        + "                \"hit_count\" : 0,\n"
        + "                \"miss_count\" : 4370\n"
        + "              }\n"
        + "            },\n"
        + "            \"primaries\" : {\n"
        + "              \"docs\" : {\n"
        + "                \"count\" : 1414978194\n"
        + "              },\n"
        + "              \"store\" : {\n"
        + "                \"size_in_bytes\" : 615609253702\n"
        + "              },\n"
        + "              \"indexing\" : {\n"
        + "                \"index_total\" : 0,\n"
        + "                \"index_time_in_millis\" : 0,\n"
        + "                \"throttle_time_in_millis\" : 0\n"
        + "              },\n"
        + "              \"search\" : {\n"
        + "                \"query_total\" : 5095,\n"
        + "                \"query_time_in_millis\" : 233865\n"
        + "              },\n"
        + "              \"merges\" : {\n"
        + "                \"total_size_in_bytes\" : 894157947088\n"
        + "              },\n"
        + "              \"refresh\" : {\n"
        + "                \"total_time_in_millis\" : 3329,\n"
        + "                \"external_total_time_in_millis\" : 29369\n"
        + "              },\n"
        + "              \"query_cache\" : {\n"
        + "                \"memory_size_in_bytes\" : 76891761,\n"
        + "                \"hit_count\" : 267,\n"
        + "                \"miss_count\" : 12907,\n"
        + "                \"evictions\" : 104\n"
        + "              },\n"
        + "              \"fielddata\" : {\n"
        + "                \"memory_size_in_bytes\" : 26784,\n"
        + "                \"evictions\" : 0\n"
        + "              },\n"
        + "              \"segments\" : {\n"
        + "                \"count\" : 125,\n"
        + "                \"memory_in_bytes\" : 60176756,\n"
        + "                \"terms_memory_in_bytes\" : 7794688,\n"
        + "                \"stored_fields_memory_in_bytes\" : 155352,\n"
        + "                \"term_vectors_memory_in_bytes\" : 0,\n"
        + "                \"norms_memory_in_bytes\" : 0,\n"
        + "                \"points_memory_in_bytes\" : 0,\n"
        + "                \"doc_values_memory_in_bytes\" : 52226716,\n"
        + "                \"index_writer_memory_in_bytes\" : 0,\n"
        + "                \"version_map_memory_in_bytes\" : 0,\n"
        + "                \"fixed_bit_set_memory_in_bytes\" : 0\n"
        + "              },\n"
        + "              \"request_cache\" : {\n"
        + "                \"memory_size_in_bytes\" : 131155,\n"
        + "                \"evictions\" : 0,\n"
        + "                \"hit_count\" : 0,\n"
        + "                \"miss_count\" : 4370\n"
        + "              }\n"
        + "            }\n"
        + "          }\n"
        + "        }";

    @Setup
    public void setup() throws IOException {
        BytesReference input;
        FilterNode[] filterNodes;
        switch (type) {
            case "ben1":
                input = new BytesArray(monSource);
                Map<String, Object> monMap = XContentHelper.convertToMap(new BytesArray(monSource), true, XContentType.JSON).v2();
                Map<String, Object> newMap = Maps.flatten(monMap, false, true);
                newMap.remove("cluster_uuid");
                filterNodes = FilterNode.compile(newMap.keySet());
                break;
            case "ben2":
                input = new BytesArray(SOURCE_2);
                filterNodes = FilterNode.compile(dvFields);
                break;
            case "ben3":
                input = new BytesArray(SOURCE_3);
                filterNodes = FilterNode.compile(dvFields);
                break;
            default:
                throw new IllegalArgumentException("Unknown type [" + type + "]");
        }
        inputSource = input;
        excludesFilters = filterNodes;
    }

    @Benchmark
    public BytesReference benchmark() throws IOException {
        try (BytesStreamOutput os = new BytesStreamOutput()) {
            XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), os);
            try (
                XContentParser parser = XContentType.JSON.xContent()
                    .createParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        inputSource.streamInput(),
                        null,
                        excludesFilters
                    )
            ) {
                builder.copyCurrentStructure(parser);
                return BytesReference.bytes(builder);
            }
        }
    }
}
