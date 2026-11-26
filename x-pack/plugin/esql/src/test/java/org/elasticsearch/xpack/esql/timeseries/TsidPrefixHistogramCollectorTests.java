/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.timeseries;

import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class TsidPrefixHistogramCollectorTests extends MapperServiceTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new AggregateMetricMapperPlugin());
    }

    public void testMetricFilter() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("cpu").field("type", "aggregate_metric_double").array("metrics", "min", "max");
            b.endObject();
            b.startObject("memory").field("type", "long").endObject();
        }));
        withLuceneIndex(mapperService, iw -> {
            iw.addDocument(
                mapperService.documentMapper()
                    .parse(source(b -> b.startObject("cpu").field("min", 1).field("max", 2).endObject()))
                    .rootDoc()
            );
            iw.addDocument(mapperService.documentMapper().parse(source(b -> b.field("memory", 3))).rootDoc());
            iw.addDocument(
                mapperService.documentMapper()
                    .parse(source(b -> b.startObject("cpu").field("min", 2).field("max", 5).endObject()))
                    .rootDoc()
            );
        }, reader -> {
            IndexSearcher searcher = newSearcher(reader);
            SearchExecutionContext context = createSearchExecutionContext(mapperService, searcher);
            // the values live in the cpu.min and cpu.max sub-fields, so the parent name alone matches nothing
            assertThat(searcher.count(new FieldExistsQuery("cpu")), equalTo(0));
            assertThat(searcher.count(TsidPrefixHistogramCollector.metricFilter(List.of("cpu"), context)), equalTo(2));
            assertThat(searcher.count(TsidPrefixHistogramCollector.metricFilter(List.of("memory"), context)), equalTo(1));
            assertThat(searcher.count(TsidPrefixHistogramCollector.metricFilter(List.of("cpu", "memory"), context)), equalTo(3));
            assertThat(searcher.count(TsidPrefixHistogramCollector.metricFilter(List.of("unknown"), context)), equalTo(0));
        });
    }
}
