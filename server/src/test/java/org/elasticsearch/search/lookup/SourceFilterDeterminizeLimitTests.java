/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceFieldMetrics;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
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

        SourceFilter filter = new SourceFilter(patterns(40, "*group_N.field*"), null);
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

    /**
     * Extra {@code *__nomatch_N__*} includes cannot be determinized, so the second filter uses glob
     * matching; they do not match the paths under test, so results must agree with the compiled filter.
     */
    public void testFallbackMatchesAutomaton() {
        String[] includes = { "title", "obj", "prefix*", "*suffix", "a*b" };
        String[] excludes = { "obj.secret", "*private*" };
        SourceFilter compiled = new SourceFilter(includes, excludes);

        String[] uncompilableIncludes = new String[includes.length + 40];
        System.arraycopy(includes, 0, uncompilableIncludes, 0, includes.length);
        System.arraycopy(patterns(40, "*__nomatch_N__*"), 0, uncompilableIncludes, includes.length, 40);
        SourceFilter fallback = new SourceFilter(uncompilableIncludes, excludes);

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
            assertThat(path, fallback.isExplicitlyIncluded(path), equalTo(compiled.isExplicitlyIncluded(path)));
            assertThat(path, fallback.isPathFiltered(path, false), equalTo(compiled.isPathFiltered(path, false)));
        }
    }

    /**
     * Glob-match the path and every ancestor prefix, then apply includes minus excludes. Exercises both
     * the compiled automaton and the fallback across randomized pattern sets.
     */
    public void testFallbackAgreesWithBruteForceReference() {
        boolean sawCompiled = false;
        boolean sawFallback = false;
        List<String> paths = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            paths.add(randomDottedPath());
        }
        for (int iter = 0; iter < 30; iter++) {
            boolean hard = randomBoolean() || (sawFallback == false && iter >= 15);
            String[] includes = hard ? patterns(40, "*group_N.field*") : randomPatterns(randomIntBetween(5, 20));
            String[] excludes = randomBoolean() ? randomPatterns(randomIntBetween(0, 5)) : new String[0];
            SourceFilter filter = new SourceFilter(includes, excludes);
            boolean compiled;
            try {
                new SourceFilter(includes, excludes).filterMap(Source.empty(null));
                compiled = true;
                sawCompiled = true;
            } catch (IllegalArgumentException e) {
                compiled = false;
                sawFallback = true;
            }
            for (String path : paths) {
                assertThat(path, filter.isPathFiltered(path, false), equalTo(referenceIsPathFiltered(includes, excludes, path)));
                assertThat(path, filter.isExplicitlyIncluded(path), equalTo(referenceIsExplicitlyIncluded(includes, path)));
            }
            assertThat("iteration compiled=" + compiled, compiled || hard, equalTo(true));
        }
        assertTrue("compiled branch must run", sawCompiled);
        assertTrue("fallback branch must run", sawFallback);
    }

    public void testFallbackSurvivesUndeterminizablePatterns() {
        SourceFilter filter = new SourceFilter(patterns(40, "*group_N.field*"), null);
        assertFalse(filter.isExplicitlyIncluded("embedding"));
        assertTrue(filter.isExplicitlyIncluded("xxgroup_3.fieldyy"));
        assertFalse(filter.isPathFiltered("xxgroup_3.fieldyy", false));
        assertTrue(filter.isPathFiltered("embedding", false));
    }

    public void testWildcardsAtBothEndsAreHandledWhenMappingHasVectors() throws IOException {
        MapperService mapperService = createMapperService(
            Settings.builder().put(IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.getKey(), true).build(),
            mapping(b -> {
                b.startObject("title").field("type", "text").endObject();
                b.startObject("embedding")
                    .field("type", "dense_vector")
                    .field("dims", 3)
                    .field("index", true)
                    .field("similarity", "l2_norm")
                    .endObject();
            })
        );
        assertFalse(mapperService.mappingLookup().syntheticVectorFields().isEmpty());

        SourceFilter filter = new SourceFilter(patterns(10, "*group_N.field*"), null);
        loadOneDocument(mapperService, filter, b -> {
            b.field("title", "hello");
            b.array("embedding", 1f, 2f, 3f);
        });
    }

    public void testEsqlStyleSourceLoaderOverFieldNamesContainingWildcards() throws IOException {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            names.add("*metric_" + i + ".value*");
        }
        MapperService mapperService = createMapperService(
            Settings.builder().put(IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.getKey(), true).build(),
            mapping(b -> {
                for (String name : names) {
                    b.startObject(name).field("type", "text").endObject();
                }
                b.startObject("embedding")
                    .field("type", "dense_vector")
                    .field("dims", 3)
                    .field("index", true)
                    .field("similarity", "l2_norm")
                    .endObject();
            })
        );
        Set<String> sourcePaths = new LinkedHashSet<>(names);
        SourceFilter filter = new SourceFilter(sourcePaths.toArray(String[]::new), null);
        Source loaded = loadOneDocument(mapperService, filter, b -> {
            b.array("embedding", 1f, 2f, 3f);
            for (int i = 0; i < 10; i++) {
                b.startObject("*metric_" + i).field("value*", "v" + i).endObject();
            }
        });
        assertThat(loaded.source(), notNullValue());
    }

    /**
     * MappingLookup.syntheticVectorsLoader iterates vector fields only. A SourceFilter test double is
     * not possible because SourceFilter is final, so this counts isPathFiltered calls on the real filter.
     */
    public void testSyntheticVectorsWalkDoesNotProbeEveryMapper() throws IOException {
        MapperService mapperService = createMapperService(
            Settings.builder()
                .put(IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.getKey(), true)
                .put("index.mapping.total_fields.limit", 5000)
                .build(),
            mapping(b -> {
                for (int i = 0; i < 1000; i++) {
                    b.startObject("f" + i).field("type", "keyword").endObject();
                }
                b.startObject("a")
                    .startObject("properties")
                    .startObject("b")
                    .startObject("properties")
                    .startObject("c")
                    .field("type", "dense_vector")
                    .field("dims", 3)
                    .field("index", true)
                    .field("similarity", "l2_norm")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject();
            })
        );
        String[] includes = new String[1000];
        for (int i = 0; i < 1000; i++) {
            includes[i] = "f" + i;
        }
        SourceFilter filter = new SourceFilter(includes, null);
        assertThat(mapperService.mappingLookup().newSourceLoader(filter, SourceFieldMetrics.NOOP, null), notNullValue());
        assertThat(filter.pathFilteredCount(), lessThan(10));
    }

    public void testIsPathFilteredForObjectsStillThrowsWhenUncompilable() {
        SourceFilter filter = new SourceFilter(patterns(40, "*group_N.field*"), null);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> filter.isPathFiltered("x", true));
        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getCause(), instanceOf(TooComplexToDeterminizeException.class));
    }

    private static boolean referenceIsExplicitlyIncluded(String[] includes, String path) {
        return includes.length > 0 && matchesPathOrAncestor(includes, path);
    }

    private static boolean referenceIsPathFiltered(String[] includes, String[] excludes, String path) {
        boolean included = includes.length == 0 || matchesPathOrAncestor(includes, path);
        boolean excluded = excludes.length > 0 && matchesPathOrAncestor(excludes, path);
        return excluded || included == false;
    }

    private static boolean matchesPathOrAncestor(String[] patterns, String fullPath) {
        if (Regex.simpleMatch(patterns, fullPath)) {
            return true;
        }
        for (int dot = fullPath.indexOf('.'); dot >= 0; dot = fullPath.indexOf('.', dot + 1)) {
            if (Regex.simpleMatch(patterns, fullPath.substring(0, dot))) {
                return true;
            }
        }
        return false;
    }

    private String[] randomPatterns(int count) {
        String[] out = new String[count];
        for (int i = 0; i < count; i++) {
            int n = randomIntBetween(0, 20);
            out[i] = switch (randomIntBetween(0, 5)) {
                case 0 -> "group_" + n + ".field";
                case 1 -> "group_" + n + ".field*";
                case 2 -> "group_" + n + "*field";
                case 3 -> "*group_" + n + ".field";
                case 4 -> "*group_" + n + ".field*";
                case 5 -> "obj.group_" + n + ".field";
                default -> throw new AssertionError();
            };
        }
        return out;
    }

    private String randomDottedPath() {
        return switch (randomIntBetween(0, 4)) {
            case 0 -> randomAlphaOfLength(5);
            case 1 -> randomAlphaOfLength(3) + "." + randomAlphaOfLength(3);
            case 2 -> "obj.nested.deep";
            case 3 -> "group_" + randomIntBetween(0, 20) + ".field";
            case 4 -> "prefix_thing";
            default -> throw new AssertionError();
        };
    }

    private Source loadOneDocument(
        MapperService mapperService,
        SourceFilter filter,
        org.elasticsearch.core.CheckedConsumer<org.elasticsearch.xcontent.XContentBuilder, IOException> document
    ) throws IOException {
        ParsedDocument parsed = mapperService.documentMapper().parse(source(document));
        Source[] loaded = new Source[1];
        withLuceneIndex(mapperService, iw -> iw.addDocuments(parsed.docs()), reader -> {
            SourceLoader loader = mapperService.mappingLookup().newSourceLoader(filter, SourceFieldMetrics.NOOP, null);
            LeafReaderContext leaf = reader.leaves().get(0);
            int[] docs = new int[] { 0 };
            SourceLoader.Leaf sourceLeaf = loader.leaf(leaf, docs);
            LeafStoredFieldLoader sfLoader = StoredFieldLoader.create(false, loader.requiredStoredFields()).getLoader(leaf, docs);
            sfLoader.advanceTo(0);
            loaded[0] = sourceLeaf.source(sfLoader, 0);
            assertThat(loaded[0], notNullValue());
        });
        return loaded[0];
    }
}
