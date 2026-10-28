/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator.CommandDescription;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator.QuerySchema;
import org.elasticsearch.xpack.esql.generator.command.pipe.ChangePointGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.DedupGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.FillNullGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.HighlightGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.IpLocationGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.LookupJoinGenerator;
import org.elasticsearch.xpack.esql.generator.command.source.FromGenerator;
import org.elasticsearch.xpack.esql.generator.command.source.FromLoadGenerator;

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.esql.generator.command.CommandGenerator.EMPTY_DESCRIPTION;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Generators run against clusters older than the test code, so each must leave out syntax the cluster doesn't advertise.
 */
public class ClusterCapabilityGatesTests extends ESTestCase {

    private static final int ITERATIONS = 500;

    /**
     * {@code agg} picks a four-argument {@code TOP} with a bare field for both arguments in roughly one call in 200, so
     * {@link #ITERATIONS} misses it often enough to fail CI.
     */
    private static final int TOP_ITERATIONS = 10_000;

    private static final QuerySchema SCHEMA = new QuerySchema(
        List.of("employees"),
        List.of(new LookupIdx("languages_lookup", List.of(new LookupIdxColumn("language_code", "integer")))),
        List.of(),
        Set.of()
    );

    public void testLookupJoinOnCoordinatorNeedsClusterSupport() {
        List<Column> previousOutput = List.of(column("language_code", "integer"));
        Function<GenerationContext, String> lookupJoin = context -> generate(LookupJoinGenerator.INSTANCE, previousOutput, context);

        assertThat(count(lookupJoin, everything(), literal("_coordinator:")), greaterThan(0));
        assertThat(count(lookupJoin, without(Cap.LOOKUP_JOIN_ON_COORDINATOR.capabilityName()), literal("_coordinator:")), is(0));
    }

    public void testChangePointByNeedsClusterSupport() {
        List<Column> previousOutput = List.of(column("@timestamp", "date"), column("value", "long"), column("host", "keyword"));
        Function<GenerationContext, String> changePoint = context -> generate(ChangePointGenerator.INSTANCE, previousOutput, context);

        assertThat(count(changePoint, everything(), literal(" BY ")), greaterThan(0));
        assertThat(count(changePoint, without(Cap.CHANGE_POINT_BY.capabilityName()), literal(" BY ")), is(0));
    }

    public void testDedupNeedsClusterSupport() {
        assertSkipped(DedupGenerator.INSTANCE, Cap.DEDUP_COMMAND);
    }

    public void testFillNullNeedsClusterSupport() {
        assertSkipped(FillNullGenerator.INSTANCE, Cap.FILLNULL);
    }

    public void testHighlightNeedsClusterSupport() {
        assertSkipped(HighlightGenerator.INSTANCE, Cap.HIGHLIGHT_V6);
    }

    public void testIpLocationNeedsClusterSupport() {
        assertSkipped(IpLocationGenerator.INSTANCE, Cap.IP_LOCATION_COMMAND);
    }

    public void testFromApproximationNeedsClusterSupport() {
        Function<GenerationContext, String> from = context -> generate(FromGenerator.INSTANCE, List.of(), context);

        assertThat(count(from, everything(), literal(FromGenerator.SET_APPROXIMATION_PREFIX)), greaterThan(0));
        assertThat(count(from, without(Cap.APPROXIMATION_V7.capabilityName()), literal(FromGenerator.SET_APPROXIMATION_PREFIX)), is(0));
    }

    public void testFromLoadApproximationNeedsClusterSupport() {
        Function<GenerationContext, String> from = context -> generate(FromLoadGenerator.INSTANCE, List.of(), context);

        assertThat(count(from, everything(), literal(FromGenerator.SET_APPROXIMATION_PREFIX)), greaterThan(0));
        assertThat(count(from, without(Cap.APPROXIMATION_V7.capabilityName()), literal(FromGenerator.SET_APPROXIMATION_PREFIX)), is(0));
    }

    public void testFromLoadAllNeedsClusterSupport() {
        Function<GenerationContext, String> from = context -> generate(FromLoadGenerator.LOAD_ALL_INSTANCE, List.of(), context);

        assertThat(count(from, everything(), literal(FromLoadGenerator.SET_LOAD_ALL_PREFIX)), is(ITERATIONS));
        assertThat(
            count(from, without(Cap.OPTIONAL_FIELDS_LOAD_ALL_V2.capabilityName()), literal(FromLoadGenerator.SET_LOAD_ALL_PREFIX)),
            is(0)
        );
    }

    public void testUnmappedFieldsLoadAllFeatureNeedsClusterSupport() {
        assertThat(GenerativeFeature.UNMAPPED_FIELDS_LOAD_ALL.requiredCapabilities(), hasItem(Cap.OPTIONAL_FIELDS_LOAD_ALL_V2));
    }

    public void testTopStringOutputFieldNeedsClusterSupport() {
        List<Column> previousOutput = List.of(column("kw", "keyword"));
        Function<GenerationContext, String> agg = context -> EsqlQueryGenerator.agg(previousOutput, List.of(), context);
        Pattern keywordOutputField = Pattern.compile("top\\(kw, \\d, \"(asc|desc)\", kw\\)");

        assertThat(count(agg, everything(), keywordOutputField, TOP_ITERATIONS), greaterThan(0));
        assertThat(count(agg, without(EsqlQueryGenerator.TOP_OUTPUT_FIELD_STRING_CAPABILITY), keywordOutputField, TOP_ITERATIONS), is(0));
    }

    /**
     * The generator gates on this capability by name, which the compiler can't check, so a rename would silently stop
     * {@code main} from generating keyword and text output fields for {@code TOP}.
     */
    public void testTopStringOutputFieldCapabilityExists() {
        Set<String> capabilities = EsqlCapabilities.capabilities(new EsqlFunctionRegistry(), true).capabilities();
        assertThat(capabilities, hasItem(EsqlQueryGenerator.TOP_OUTPUT_FIELD_STRING_CAPABILITY));
    }

    private static void assertSkipped(CommandGenerator generator, Cap cap) {
        List<Column> previousOutput = List.of(column("message", "text"), column("client_ip", "ip"), column("emp_no", "integer"));
        GenerationContext context = without(cap.capabilityName());
        for (int i = 0; i < ITERATIONS; i++) {
            assertThat(generator.generate(List.of(), previousOutput, SCHEMA, null, context), sameInstance(EMPTY_DESCRIPTION));
        }
    }

    private static int count(Function<GenerationContext, String> generate, GenerationContext context, Pattern pattern) {
        return count(generate, context, pattern, ITERATIONS);
    }

    private static int count(Function<GenerationContext, String> generate, GenerationContext context, Pattern pattern, int iterations) {
        int matches = 0;
        for (int i = 0; i < iterations; i++) {
            if (pattern.matcher(generate.apply(context)).find()) {
                matches++;
            }
        }
        return matches;
    }

    private static Pattern literal(String text) {
        return Pattern.compile(Pattern.quote(text));
    }

    private static String generate(CommandGenerator generator, List<Column> previousOutput, GenerationContext context) {
        CommandDescription description = generator.generate(List.of(), previousOutput, SCHEMA, null, context);
        return description.commandString();
    }

    private static GenerationContext everything() {
        return context(capability -> true);
    }

    private static GenerationContext without(String unsupported) {
        return context(capability -> capability.equals(unsupported) == false);
    }

    private static GenerationContext context(Predicate<String> clusterCapabilities) {
        return GenerationContext.root(Set.of(), clusterCapabilities);
    }

    private static Column column(String name, String type) {
        return new Column(name, type, List.of());
    }
}
