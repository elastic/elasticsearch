/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.SourceFieldMetrics;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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

    /** Verifies the pattern shapes that exceed Lucene's default determinization limit. */
    public void testWhichPatternShapesExceedTheDeterminizeLimit() {
        // Wildcards at both ends exceed the limit with ten patterns.
        Regex.simpleMatchToAutomaton(patterns(5, "*group_N.field*"));
        expectThrows(TooComplexToDeterminizeException.class, () -> Regex.simpleMatchToAutomaton(patterns(10, "*group_N.field*")));

        // A leading wildcard also exceeds the limit, but needs more patterns.
        Regex.simpleMatchToAutomaton(patterns(100, "*group_N.field"));
        expectThrows(TooComplexToDeterminizeException.class, () -> Regex.simpleMatchToAutomaton(patterns(200, "*group_N.field")));

        // Prefix, infix, and literal patterns remain below the limit.
        Regex.simpleMatchToAutomaton(patterns(500, "group_N.field*"));
        Regex.simpleMatchToAutomaton(patterns(500, "group_N*field"));
        Regex.simpleMatchToAutomaton(patterns(5000, "group_N.field"));
    }

    /** Does not compile the source filter when there are no vectors to restore. */
    public void testSourceFilterIsNotCompiledWhenMappingHasNoVectors() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> b.startObject("title").field("type", "text").endObject()));
        assertTrue(mapperService.mappingLookup().syntheticVectorFields().isEmpty());

        SourceFilter filter = new SourceFilter(patterns(10, "*group_N.field*"), null);
        assertThat(mapperService.mappingLookup().newSourceLoader(filter, SourceFieldMetrics.NOOP, null), notNullValue());
    }

    /** Handles ES|QL source paths whose field names contain wildcard characters. */
    public void testEsqlStyleSourceLoaderOverFieldNamesContainingWildcards() throws IOException {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            names.add("*metric_" + i + ".value*");
        }
        MapperService mapperService = createMapperService(mapping(b -> {
            for (String name : names) {
                b.startObject(name).field("type", "text").endObject();
            }
        }));
        assertEquals(names.size(), mapperService.mappingLookup().getMapping().getRoot().getMappers().size());

        // Build the source filter from ES|QL's resolved source paths.
        Set<String> sourcePaths = new LinkedHashSet<>(names);
        SourceFilter filter = new SourceFilter(sourcePaths.toArray(String[]::new), null);

        assertThat(mapperService.mappingLookup().newSourceLoader(filter, SourceFieldMetrics.NOOP, null), notNullValue());
    }

    /** Handles complex filters while walking mappings with vectors to restore. */
    public void testWildcardsAtBothEndsAreHandledWhenMappingHasVectors() throws IOException {
        MapperService mapperService = createMapperService(
            Settings.builder().put(IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.getKey(), true).build(),
            mapping(b -> {
                b.startObject("title").field("type", "text").endObject();
                // Indexed vectors require a synthetic vectors loader.
                b.startObject("embedding").field("type", "dense_vector").field("dims", 3).field("index", true).endObject();
            })
        );
        assertFalse(mapperService.mappingLookup().syntheticVectorFields().isEmpty());

        SourceFilter filter = new SourceFilter(patterns(10, "*group_N.field*"), null);
        assertThat(mapperService.mappingLookup().newSourceLoader(filter, SourceFieldMetrics.NOOP, null), notNullValue());
    }

    /** Reports patterns that cannot be compiled as a bad request. */
    public void testCompilationStillReportsUndeterminizablePatternsAsABadRequest() {
        SourceFilter filter = new SourceFilter(patterns(10, "*group_N.field*"), null);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> filter.filterMap(Source.empty(null)));
        assertThat(e.getMessage(), containsString("[10] field patterns are too complex to compile"));
        assertThat(e.getCause(), instanceOf(TooComplexToDeterminizeException.class));
        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.BAD_REQUEST));
    }

    /** Verifies that direct matching agrees with the compiled automaton. */
    public void testUncompiledProbesAgreeWithTheAutomaton() {
        String[] includes = { "title", "obj", "prefix*", "*suffix", "a*b" };
        String[] excludes = { "obj.secret", "*private*" };
        SourceFilter filter = new SourceFilter(includes, excludes);

        for (String path : List.of(
            "title",
            "titles",
            "obj",
            "obj.nested",
            "obj.nested.deep",
            "obj.secret",
            "prefix_thing",
            "thing_suffix",
            "axxb",
            "unrelated",
            "obj.private_thing",
            "a.b"
        )) {
            assertThat(path, filter.isExplicitlyIncludedWithoutCompiling(path), equalTo(filter.isExplicitlyIncluded(path)));
            assertThat(path, filter.isPathFilteredWithoutCompiling(path), equalTo(filter.isPathFiltered(path, false)));
        }
    }

    /** Direct matching works when the pattern set cannot be determinized. */
    public void testUncompiledProbesSurviveUndeterminizablePatterns() {
        SourceFilter filter = new SourceFilter(patterns(10, "*group_N.field*"), null);
        expectThrows(IllegalArgumentException.class, () -> filter.isExplicitlyIncluded("embedding"));

        assertFalse(filter.isExplicitlyIncludedWithoutCompiling("embedding"));
        assertTrue(filter.isExplicitlyIncludedWithoutCompiling("xxgroup_3.fieldyy"));
        assertFalse(filter.isPathFilteredWithoutCompiling("xxgroup_3.fieldyy"));
    }

    /** Uses the source-filter determinization limit, not Lucene's default. */
    public void testCompilingOnceLiftsTheCeiling() {
        String[] patterns = patterns(200, "*group_N.field");

        TooComplexToDeterminizeException e = expectThrows(
            TooComplexToDeterminizeException.class,
            () -> Regex.simpleMatchToAutomaton(patterns)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("more than 10000 effort"));

        Automaton tail = Operations.union(
            Automata.makeEmptyString(),
            Operations.concatenate(Automata.makeChar('.'), Automata.makeAnyString())
        );
        Operations.determinize(Operations.concatenate(Regex.simpleMatchToNonDeterminizedAutomaton(patterns), tail), 50_000);
        new SourceFilter(patterns, null).filterMap(Source.empty(null));
    }
}
