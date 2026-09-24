/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.highlight;

import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.IndexAnalyzerGroup;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.plan.logical.highlight.HighlightAnalyzers.Resolved;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_ANALYSIS_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.TextEsField.DEFAULT_POSITION_INCREMENT_GAP;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

public class HighlightAnalyzersTests extends ESTestCase {

    /**
     * Without WITH, each ON field keeps its own analyzer. Mapped text uses the mapping name, TO_TEXT uses
     * its declaration, and anything else uses {@code standard}. Map order matches ON order.
     */
    public void testEachFieldPicksItsOwnAnalyzerInOnOrder() {
        Map<String, NamedAnalyzer> resolved = resolve(
            List.of(
                textField("title", "whitespace", 0),
                textField("body", null),
                declaredField("note", "simple"),
                declaredField("other", null),
                getFieldAttribute("tag", KEYWORD)
            ),
            null
        ).analysisGroups().getFirst();
        assertThat(List.copyOf(resolved.keySet()), contains("title", "body", "note", "other", "tag"));
        assertThat(
            resolved.values().stream().map(NamedAnalyzer::name).toList(),
            contains("whitespace", "standard", "simple", "standard", "standard")
        );
        assertThat(resolved.get("title").getPositionIncrementGap("title"), equalTo(0));
    }

    public void testWithAnalyzerOverridesMappingAndDeclared() {
        Resolved resolved = resolve(List.of(textField("title", "whitespace"), declaredField("note", "simple")), "keyword");
        assertThat(
            resolved.analysisGroups().getFirst().values().stream().map(NamedAnalyzer::name).toList(),
            contains("keyword", "keyword")
        );
    }

    // Mapping analyzer this node cannot build. Resolve returns standard instead of failing the query.
    public void testUnknownMappingAnalyzerFallsBackToStandard() {
        assertThat(names(textField("title", "my_index_analyzer")), contains("standard"));
    }

    // Same as above, but confirm a warning is emitted through the sink and names the field and analyzer.
    public void testUnknownMappingAnalyzerEmitsFallbackWarning() {
        List<String> warnings = new ArrayList<>();
        resolve(List.of(textField("title", "my_index_analyzer")), null, false, warnings);
        assertThat(warnings, hasSize(1));
        assertThat(warnings.get(0), containsString("HIGHLIGHT on [title] falls back to [standard]"));
        assertThat(warnings.get(0), containsString("analyzer [my_index_analyzer]"));
        assertThat(warnings.get(0), containsString("WITH {\"analyzer\": <registered analyzer>}"));
    }

    // Two indices disagree on the analyzer and the row's index is not available: standard and a warning.
    public void testMultiIndexAnalyzerConflictFallsBackAndWarns() {
        List<String> warnings = new ArrayList<>();
        Resolved resolved = resolve(List.of(conflictingField("title")), null, false, warnings);
        assertThat(resolved.analysisGroups(), hasSize(1));
        assertThat(resolved.analysisGroups().getFirst().get("title").name(), equalTo("standard"));
        assertThat(warnings, hasItem(containsString("indices disagree on the analyzer")));
    }

    /**
     * With the row's index available, each index gets its own analyzer and no warning. {@code body} agrees everywhere,
     * so the two {@code title} groups yield two analysis groups besides the default, which keeps {@code standard} for rows
     * from an index outside the groups.
     */
    public void testMultiIndexAnalyzerConflictResolvesPerIndex() {
        List<String> warnings = new ArrayList<>();
        Resolved resolved = resolve(List.of(conflictingField("title"), textField("body", "simple")), null, true, warnings);
        assertThat(warnings, empty());
        assertThat(resolved.groupByIndex(), equalTo(Map.of("books", 1, "books_english", 2, "books_english_2", 2)));
        assertThat(names(resolved.analysisGroups().get(0)), contains("standard", "simple"));
        assertThat(names(resolved.analysisGroups().get(1)), contains("whitespace", "simple"));
        assertThat(names(resolved.analysisGroups().get(2)), contains("stop", "simple"));
        assertThat(resolved.analysisGroups().get(1).get("title").getPositionIncrementGap("title"), equalTo(0));
    }

    // Groups with the same analyzer name but a different gap are distinct analysis groups: NamedAnalyzer#equals ignores the gap.
    public void testSameAnalyzerDifferentGapKeepsSeparateAnalysisGroups() {
        FieldAttribute field = textFieldWithGroups(
            "title",
            new IndexAnalyzerGroup("whitespace", false, 0, Set.of("a")),
            new IndexAnalyzerGroup("whitespace", false, 50, Set.of("b"))
        );
        Resolved resolved = resolve(List.of(field), null, true, new ArrayList<>());
        assertThat(resolved.groupByIndex(), equalTo(Map.of("a", 1, "b", 2)));
        assertThat(resolved.analysisGroups().get(1).get("title").getPositionIncrementGap("title"), equalTo(0));
        assertThat(resolved.analysisGroups().get(2).get("title").getPositionIncrementGap("title"), equalTo(50));
    }

    /**
     * Indices whose analyzer was withheld, not reported, or is not registered here use standard and are named in one
     * warning per group. Those end up in the first analysis group, so only {@code books_english} is listed.
     */
    public void testUnresolvableGroupUsesStandardAndNamesIndices() {
        List<String> warnings = new ArrayList<>();
        FieldAttribute field = textFieldWithGroups(
            "title",
            new IndexAnalyzerGroup("stop", false, DEFAULT_POSITION_INCREMENT_GAP, Set.of("books_english")),
            new IndexAnalyzerGroup(null, true, DEFAULT_POSITION_INCREMENT_GAP, Set.of("custom_b", "custom_a")),
            new IndexAnalyzerGroup(null, false, DEFAULT_POSITION_INCREMENT_GAP, Set.of("old_remote:books")),
            new IndexAnalyzerGroup("my_plugin_analyzer", false, DEFAULT_POSITION_INCREMENT_GAP, Set.of("plugin"))
        );
        Resolved resolved = resolve(List.of(field), null, true, warnings);
        assertThat(resolved.groupByIndex(), equalTo(Map.of("books_english", 1)));
        assertThat(resolved.analysisGroups(), hasSize(2));
        assertThat(
            warnings,
            contains(
                containsString(
                    "HIGHLIGHT on [title] falls back to [standard] for indices [custom_a, custom_b]: its analyzer is defined in"
                ),
                containsString("for indices [old_remote:books]: the node holding it did not report its analyzer"),
                containsString("for indices [plugin]: analyzer [my_plugin_analyzer] is not registered")
            )
        );
    }

    // The shard withheld an index.analysis name, so resolve falls back to standard and warns.
    public void testIndexLocalAnalyzerFallsBackAndWarns() {
        List<String> warnings = new ArrayList<>();
        Resolved resolved = resolve(List.of(unknownAnalyzerField(TextEsField.UnknownAnalyzer.INDEX_LOCAL)), null, true, warnings);
        assertThat(resolved.analysisGroups().getFirst().get("title").name(), equalTo("standard"));
        assertThat(warnings, hasItem(containsString("its analyzer is defined in the index settings")));
    }

    // WITH takes precedence; a mapping analyzer HIGHLIGHT cannot use must not warn once the user has set WITH.
    public void testWithAnalyzerSuppressesUnknownAnalyzerWarning() {
        for (var field : List.of(unknownAnalyzerField(TextEsField.UnknownAnalyzer.INDEX_LOCAL), conflictingField("title"))) {
            List<String> warnings = new ArrayList<>();
            Resolved resolved = resolve(List.of(field), "keyword", randomBoolean(), warnings);
            assertThat(warnings, hasSize(0));
            assertThat(resolved.analysisGroups(), hasSize(1));
        }
    }

    /**
     * A FORK or UNION ALL column is a {@link ReferenceAttribute}, so its mapping comes from the carried map, by name. A
     * carried conflict resolves per index just like a mapped field's.
     */
    public void testMergedColumnUsesCarriedMapping() {
        ReferenceAttribute title = declaredField("title", null);
        List<String> warnings = new ArrayList<>();
        Map<String, TextEsField> whitespace = Map.of("title", textMapping("title", "whitespace", 0, TextEsField.UnknownAnalyzer.NONE));
        Resolved resolved = resolve(List.of(title), whitespace, null, false, warnings);
        assertThat(names(resolved.analysisGroups().getFirst()), contains("whitespace"));
        assertThat(resolved.analysisGroups().getFirst().get("title").getPositionIncrementGap("title"), equalTo(0));

        Map<String, TextEsField> conflicting = Map.of("title", (TextEsField) conflictingField("title").field());
        resolved = resolve(List.of(title), conflicting, null, true, warnings);
        assertThat(resolved.groupByIndex(), equalTo(Map.of("books", 1, "books_english", 2, "books_english_2", 2)));
        assertThat(warnings, empty());
    }

    // Branches that disagree on the column's mapping, or where one computes it, fall back to standard and say why.
    public void testMergedColumnBranchConflictFallsBackAndWarns() {
        List<String> warnings = new ArrayList<>();
        Map<String, TextEsField> mappings = Map.of(
            "title",
            textMapping("title", null, DEFAULT_POSITION_INCREMENT_GAP, TextEsField.UnknownAnalyzer.BRANCH_CONFLICT)
        );
        Resolved resolved = resolve(List.of(declaredField("title", null)), mappings, null, randomBoolean(), warnings);
        assertThat(names(resolved.analysisGroups().getFirst()), contains("standard"));
        assertThat(
            warnings,
            contains(
                containsString(
                    "HIGHLIGHT on [title] falls back to [standard]: the FORK or UNION ALL branches disagree on the analyzer for this column"
                )
            )
        );
    }

    /** A {@code title} field whose analyzer name never reached the coordinator, for the given reason. */
    private static FieldAttribute unknownAnalyzerField(TextEsField.UnknownAnalyzer unknown) {
        return textField("title", null, DEFAULT_POSITION_INCREMENT_GAP, unknown);
    }

    /** {@code books} uses {@code whitespace} with no gap, the two english indices share {@code stop} (prebuilt; {@code english} needs a plugin). */
    private static FieldAttribute conflictingField(String name) {
        return textFieldWithGroups(
            name,
            new IndexAnalyzerGroup("whitespace", false, 0, Set.of("books")),
            new IndexAnalyzerGroup("stop", false, DEFAULT_POSITION_INCREMENT_GAP, Set.of("books_english", "books_english_2"))
        );
    }

    public void testUnknownCommandAndDeclaredAnalyzersThrow() {
        expectThrows(InvalidArgumentException.class, () -> resolve(List.of(textField("title", "whitespace")), "nope"));
        expectThrows(InvalidArgumentException.class, () -> names(declaredField("note", "nope")));
    }

    private static Resolved resolve(List<? extends NamedExpression> fields, String withAnalyzer) {
        return resolve(fields, withAnalyzer, false, new ArrayList<>());
    }

    private static Resolved resolve(List<? extends NamedExpression> fields, String withAnalyzer, boolean perIndex, List<String> warnings) {
        return resolve(fields, Map.of(), withAnalyzer, perIndex, warnings);
    }

    private static Resolved resolve(
        List<? extends NamedExpression> fields,
        Map<String, TextEsField> fieldMappings,
        String withAnalyzer,
        boolean perIndex,
        List<String> warnings
    ) {
        return HighlightAnalyzers.resolve(fields, fieldMappings, withAnalyzer, TEST_ANALYSIS_REGISTRY, perIndex, warnings::add);
    }

    private static List<String> names(NamedExpression... onFields) {
        return names(resolve(List.of(onFields), null).analysisGroups().getFirst());
    }

    private static List<String> names(Map<String, NamedAnalyzer> fieldAnalyzers) {
        return fieldAnalyzers.values().stream().map(NamedAnalyzer::name).toList();
    }

    private static FieldAttribute textField(String name, String analyzerName) {
        return textField(name, analyzerName, DEFAULT_POSITION_INCREMENT_GAP);
    }

    private static FieldAttribute textField(String name, String analyzerName, int positionIncrementGap) {
        return textField(name, analyzerName, positionIncrementGap, TextEsField.UnknownAnalyzer.NONE);
    }

    private static FieldAttribute textField(String name, String analyzerName, int gap, TextEsField.UnknownAnalyzer unknown) {
        return new FieldAttribute(EMPTY, name, textMapping(name, analyzerName, gap, unknown));
    }

    private static TextEsField textMapping(String name, String analyzerName, int gap, TextEsField.UnknownAnalyzer unknown) {
        return new TextEsField(name, Map.of(), false, false, EsField.TimeSeriesFieldType.NONE, analyzerName, gap, unknown, null);
    }

    private static FieldAttribute textFieldWithGroups(String name, IndexAnalyzerGroup... groups) {
        return new FieldAttribute(
            EMPTY,
            name,
            new TextEsField(
                name,
                Map.of(),
                false,
                false,
                EsField.TimeSeriesFieldType.NONE,
                null,
                DEFAULT_POSITION_INCREMENT_GAP,
                TextEsField.UnknownAnalyzer.CONFLICT,
                List.of(groups)
            )
        );
    }

    private static ReferenceAttribute declaredField(String name, String analyzerName) {
        return new ReferenceAttribute(EMPTY, null, name, TEXT, Nullability.TRUE, null, false, analyzerName);
    }
}
