/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.index.mapper;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.xcontent.XContentType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class KeywordFieldMapperBenchmark {

    private MapperService mapperService;

    private SourceToParse sourceToParse;

    @Setup
    public void setUp() {
        this.mapperService = MapperServiceFactory.create("""
            {
              "_doc": {
                "dynamic": false,
                "properties": {
                  "host": {
                    "type": "keyword",
                    "store": true
                  },
                  "name": {
                    "type": "keyword"
                  },
                  "type": {
                    "type": "keyword",
                    "normalizer": "lowercase"
                  },
                  "uuid": {
                    "type": "keyword",
                    "doc_values": false
                  },
                  "version": {
                    "type": "keyword"
                  },
                  "extra_field": {
                    "type": "keyword"
                  },
                  "extra_field_text": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 1024
                      }
                    }
                  },
                  "nested_object": {
                    "properties": {
                      "keyword_nested": {
                        "type": "keyword"
                      },
                      "text_nested": {
                        "type": "text"
                      },
                      "number_nested": {
                        "type": "long"
                      }
                    }
                  }
                }
              }
            }
                       \s""");
        this.sourceToParse = new SourceToParse(UUIDs.randomBase64UUID(), new BytesArray("""
            {
              "host": "some_value",
              "name": "some_other_thing",
              "type": "some_type_thing",
              "uuid": "some_keyword_uuid",
              "version": "some_version",
              "extra_field_text": "bla bla text",
              "nested_object" : {
                "keyword_nested": "some random nested keyword",
                "text_nested": "some random nested text",
                "number_nested": 123234234
              }
            }"""), XContentType.JSON);
    }

    @Benchmark
    public List<LuceneDocument> benchmarkParseKeywordFields() {
        return mapperService.documentMapper().parse(sourceToParse).docs();
    }
}
