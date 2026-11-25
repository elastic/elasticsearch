/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.get;

import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.search.fetch.subphase.FetchFieldsContext;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/** Tests {@link ShardGetService#maybeExcludeVectorFields}, which strips vector fields from {@code _source} during get and fetch. */
public class ExcludeVectorFieldsFromSourceTests extends MapperServiceTestCase {

    /** Padding, so that fewer patterns are needed to reach the limit than short names would require. */
    private static final String FILLER = "_filler_to_pad_the_name_";

    /**
     * Builds a {@code fields} request that {@link Regex#simpleMatchToAutomaton} cannot compile, which is what a client sends when it asks
     * for a long list of fields next to a `*`. Compiling the requested patterns is exactly what the code under test must not do.
     * <p>
     * Concrete names alone never exceed the limit, at any count: {@code Automata#makeStringUnion} returns a minimal DFA and
     * {@code Operations#determinize} short circuits on it. It is the match-all pattern that forces the subset construction, where every
     * state of the union pairs with the always accepting state of the wildcard, which puts the ceiling at around 50000 states. Each name
     * carries a unique head and a unique tail so that neither prefix nor suffix minimization collapses it, and is padded so that fewer
     * of them are needed to reach that ceiling.
     */
    private static List<FieldAndFormat> fieldPatternsOverDeterminizeLimit() {
        List<FieldAndFormat> fields = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            String index = Strings.format("%05d", i);
            fields.add(new FieldAndFormat("f" + index + FILLER + new StringBuilder(index).reverse(), null));
        }
        fields.add(new FieldAndFormat("*", null));
        return fields;
    }

    /**
     * Guards the fixture above: it only covers anything as long as the patterns it builds really are too many to compile. Without this the
     * tests below keep passing if that limit ever moves, while no longer exercising the case they were written for.
     */
    public void testFieldPatternsCannotBeCompiledIntoAnAutomaton() {
        String[] patterns = fieldPatternsOverDeterminizeLimit().stream().map(f -> f.field).toArray(String[]::new);
        expectThrows(TooComplexToDeterminizeException.class, () -> Regex.simpleMatchToAutomaton(patterns));
    }

    private MapperService mapperServiceWithVectorField() throws IOException {
        return createMapperService(mapping(b -> {
            b.startObject("title").field("type", "keyword").endObject();
            b.startObject("embedding").field("type", "dense_vector").field("dims", 3).field("index", false).endObject();
        }));
    }

    /**
     * A mapping without vector embeddings has nothing to exclude from {@code _source}, so the requested field patterns must never be
     * compiled. Otherwise a request asking for a large number of fields fails with a shard error even though the filtering step would
     * have been a no-op.
     */
    public void testFieldPatternsAreNotCompiledWhenMappingHasNoVectors() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> { b.startObject("title").field("type", "keyword").endObject(); }));

        List<FieldAndFormat> fields = fieldPatternsOverDeterminizeLimit();
        var result = ShardGetService.maybeExcludeVectorFields(
            mapperService.mappingLookup(),
            mapperService.getIndexSettings(),
            null,
            new FetchFieldsContext(fields)
        );
        assertThat(result.v1(), nullValue());
        assertThat(result.v2(), nullValue());
    }

    /**
     * The requested field patterns are matched against the vector fields one at a time, so a request carrying more patterns than can be
     * compiled into a single automaton is still served. The match-all among them counts as asking for the vector field, which is
     * therefore excluded late, exactly as it is when it is the only pattern.
     */
    public void testManyRequestedFieldsWithVectorFieldInMapping() throws IOException {
        MapperService mapperService = mapperServiceWithVectorField();

        List<FieldAndFormat> fields = fieldPatternsOverDeterminizeLimit();
        var result = ShardGetService.maybeExcludeVectorFields(
            mapperService.mappingLookup(),
            mapperService.getIndexSettings(),
            null,
            new FetchFieldsContext(fields)
        );
        assertThat(result.v1(), notNullValue());
        assertThat(result.v1().excludes(), arrayContaining("embedding"));
        assertThat(result.v2(), nullValue());
    }

    /**
     * A match-all pattern counts as asking for the vector field, so it must be excluded late rather than up front: the value stays
     * loadable for the sub-fetch phases that return it, and is dropped only when {@code _source} itself is rendered.
     */
    public void testMatchAllRequestedFieldPatternExcludesVectorsLate() throws IOException {
        MapperService mapperService = mapperServiceWithVectorField();

        var result = ShardGetService.maybeExcludeVectorFields(
            mapperService.mappingLookup(),
            mapperService.getIndexSettings(),
            null,
            new FetchFieldsContext(List.of(new FieldAndFormat("*", null)))
        );
        assertThat(result.v1(), notNullValue());
        assertThat(result.v1().excludes(), arrayContaining("embedding"));
        assertThat(result.v2(), nullValue());
    }

    /**
     * The mapping holds a vector field but the request carries no {@code fields} option, which is how ES|QL and the get API call this.
     * There is nothing to match the vector field against, so it is excluded up front rather than late.
     */
    public void testVectorFieldInMappingIsExcludedWhenNoFieldsRequested() throws IOException {
        MapperService mapperService = mapperServiceWithVectorField();

        var result = ShardGetService.maybeExcludeVectorFields(mapperService.mappingLookup(), mapperService.getIndexSettings(), null, null);
        assertThat(result.v2(), notNullValue());
        assertThat(result.v2().getExcludes(), arrayContaining("embedding"));
    }

    /**
     * A vector field requested through the {@code fields} option is excluded late, so it stays available to sub-fetch phases. A plain
     * list of field names needs no automaton, so this path must work regardless of how many fields are requested.
     */
    public void testRequestedVectorFieldIsExcludedLate() throws IOException {
        MapperService mapperService = mapperServiceWithVectorField();

        List<FieldAndFormat> fields = new ArrayList<>();
        fields.add(new FieldAndFormat("title", null));
        fields.add(new FieldAndFormat("embedding", null));

        var result = ShardGetService.maybeExcludeVectorFields(
            mapperService.mappingLookup(),
            mapperService.getIndexSettings(),
            null,
            new FetchFieldsContext(fields)
        );
        assertThat(result.v1(), notNullValue());
        assertThat(result.v1().excludes(), arrayContaining("embedding"));
        assertThat(result.v2(), nullValue());
    }
}
