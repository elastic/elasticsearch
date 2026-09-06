/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.SourceFieldMetrics;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

/** Regression tests for https://github.com/elastic/elasticsearch/issues/142554. */
public class SourceFilterDeterminizeLimitTests extends MapperServiceTestCase {

    private static String[] patterns(int count, String shape) {
        String[] out = new String[count];
        for (int i = 0; i < count; i++) {
            out[i] = shape.replace("N", Integer.toString(i));
        }
        return out;
    }

    /**
     * Wide margins around the measured failure points: unions of {@code *x*} fail around 10 patterns,
     * and unions of {@code *x} fail around 200.
     */
    public void testWhichPatternShapesExceedTheDeterminizeLimit() {
        Regex.simpleMatchToAutomaton(patterns(3, "*group_N.field*"));
        expectThrows(TooComplexToDeterminizeException.class, () -> Regex.simpleMatchToAutomaton(patterns(40, "*group_N.field*")));

        Regex.simpleMatchToAutomaton(patterns(50, "*group_N.field"));
        expectThrows(TooComplexToDeterminizeException.class, () -> Regex.simpleMatchToAutomaton(patterns(400, "*group_N.field")));
    }

    /** Does not compile the source filter when there are no vectors to restore. */
    public void testSourceFilterIsNotCompiledWhenMappingHasNoVectors() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> b.startObject("title").field("type", "text").endObject()));
        assertTrue(mapperService.mappingLookup().syntheticVectorFields().isEmpty());

        SourceFilter filter = new SourceFilter(patterns(10, "*group_N.field*"), null);
        assertThat(mapperService.mappingLookup().newSourceLoader(filter, SourceFieldMetrics.NOOP, null), notNullValue());
    }

    /** Reports patterns that cannot be compiled as a bad request. */
    public void testCompilationStillReportsUndeterminizablePatternsAsABadRequest() {
        SourceFilter filter = new SourceFilter(patterns(40, "*group_N.field*"), null);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> filter.filterMap(Source.empty(null)));
        assertThat(e.getMessage(), containsString("too complex"));
        assertThat(e.getCause(), instanceOf(TooComplexToDeterminizeException.class));
        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.BAD_REQUEST));
    }

    /**
     * 200 leading-wildcard patterns exceed Lucene's default 10_000 effort, but compile once at the
     * source-filter limit of 50_000.
     */
    public void testCompilingOnceLiftsTheCeiling() {
        String[] patterns = patterns(200, "*group_N.field");
        expectThrows(TooComplexToDeterminizeException.class, () -> Regex.simpleMatchToAutomaton(patterns));
        new SourceFilter(patterns, null).filterMap(Source.empty(null));
    }
}
