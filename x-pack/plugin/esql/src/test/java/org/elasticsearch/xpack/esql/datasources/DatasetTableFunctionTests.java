/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.dataset.DatasetDefinitionValidator;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.tablefunction.TableFunctionArgument;
import org.elasticsearch.xpack.esql.tablefunction.TableFunctionInvocation;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * What {@code DATASET(...)} accepts and refuses before anything is authorized or bound. The binding itself is
 * covered where the validator is ({@code InlineDefinitionValidationParityTests}) and where the node-side rebind is
 * ({@link DatasetConfigResolverTests}); nothing here needs a cluster.
 */
public class DatasetTableFunctionTests extends ESTestCase {

    private static final DatasetTableFunction FUNCTION = new DatasetTableFunction(null, new DatasetDefinitionValidator(Map.of()));

    public void testTheShapeIsADataSourceAndADefinition() {
        assertThat(FUNCTION.signature().arguments().size(), equalTo(2));

        ParsingException tooFew = expectThrows(ParsingException.class, () -> FUNCTION.signature().check(call(name("s3"))));
        assertThat(tooFew.getMessage(), containsString("[dataset] takes [data source, definition], found [1]"));

        ParsingException tooMany = expectThrows(
            ParsingException.class,
            () -> FUNCTION.signature().check(call(name("s3"), name("s3://b/*.csv"), namedValues(Map.of())))
        );
        assertThat(tooMany.getMessage(), containsString("found [3]"));
    }

    /** The definition may be written either way, so both spellings pass the shape check. */
    public void testTheDefinitionIsAcceptedAsAResourceOrAsNamedValues() {
        FUNCTION.signature().check(call(name("s3"), name("s3://bucket/*.csv")));
        FUNCTION.signature().check(call(name("s3"), namedValues(Map.of("resource", "s3://bucket/*.csv"))));
    }

    /** The data source is never the {@code {...}}: the read has to know which source's credentials it uses. */
    public void testTheDataSourceMustBeAName() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> FUNCTION.signature().check(call(namedValues(Map.of()), name("s3://bucket/*.csv")))
        );
        assertThat(e.getMessage(), containsString("[dataset] expects a name for [data source]"));
    }

    /**
     * One data source, never a pattern, a list, an exclusion or a remote-prefixed name.
     * <p>
     * {@code ?} and a leading {@code /} are in the list because the authorization layer reads both as patterns -
     * {@code StringMatcher} routes a grant containing either to an automaton - while {@code Regex.isSimpleMatchPattern},
     * the obvious predicate to reach for here, tests {@code *} alone. Admitting one would authorize the literal string
     * and then resolve nothing, leaving the read singular by the later exact lookup rather than by this check.
     */
    public void testTheDataSourceIsOneRegisteredName() {
        for (String plural : List.of("", "*", "logs-*", "d?1", "/d1/", "a,b", "-a", "remote:a")) {
            TableFunctionInvocation call = call(name(plural), name("s3://bucket/*.csv"));
            ParsingException e = expectThrows(ParsingException.class, () -> FUNCTION.validate(call));
            assertThat(e.getMessage(), containsString("[dataset] reads one data source, found [" + plural + "]"));
        }
    }

    public void testAPlainDataSourceNamePasses() {
        FUNCTION.validate(call(name("s3_source"), name("s3://bucket/*.csv")));
    }

    private static TableFunctionInvocation call(TableFunctionArgument... arguments) {
        return new TableFunctionInvocation(Source.EMPTY, DatasetTableFunction.NAME, List.of(arguments));
    }

    private static TableFunctionArgument name(String value) {
        return new TableFunctionArgument.Name(Source.EMPTY, value);
    }

    private static TableFunctionArgument namedValues(Map<String, Object> values) {
        return new TableFunctionArgument.NamedValues(Source.EMPTY, values);
    }
}
