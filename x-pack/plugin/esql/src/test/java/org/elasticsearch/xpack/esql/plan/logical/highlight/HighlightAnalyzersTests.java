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
import org.elasticsearch.xpack.esql.core.type.TextEsField;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_ANALYSIS_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

public class HighlightAnalyzersTests extends ESTestCase {

    /**
     * Without WITH, each ON field keeps its own analyzer. Mapped text uses the mapping name, TO_TEXT uses
     * its declaration, and anything else uses {@code standard}. Map order matches ON order.
     */
    public void testEachFieldPicksItsOwnAnalyzerInOnOrder() {
        Map<String, NamedAnalyzer> resolved = HighlightAnalyzers.resolve(
            List.of(
                textField("title", "whitespace", 0),
                textField("body", null),
                declaredField("note", "simple"),
                declaredField("other", null),
                getFieldAttribute("tag", KEYWORD)
            ),
            null,
            TEST_ANALYSIS_REGISTRY
        );
        assertThat(List.copyOf(resolved.keySet()), contains("title", "body", "note", "other", "tag"));
        assertThat(
            resolved.values().stream().map(NamedAnalyzer::name).toList(),
            contains("whitespace", "standard", "simple", "standard", "standard")
        );
        assertThat(resolved.get("title").getPositionIncrementGap("title"), equalTo(0));
    }

    public void testWithAnalyzerOverridesMappingAndDeclared() {
        Map<String, NamedAnalyzer> resolved = HighlightAnalyzers.resolve(
            List.of(textField("title", "whitespace"), declaredField("note", "simple")),
            "keyword",
            TEST_ANALYSIS_REGISTRY
        );
        assertThat(resolved.values().stream().map(NamedAnalyzer::name).toList(), contains("keyword", "keyword"));
    }

    // Mapping analyzer this node cannot build. Resolve returns standard instead of failing the query.
    public void testUnknownMappingAnalyzerFallsBackToStandard() {
        assertThat(names(textField("title", "my_index_analyzer")), contains("standard"));
    }

    // Same as above, but confirm a warning is emitted through the sink and names the field and analyzer.
    public void testUnknownMappingAnalyzerEmitsFallbackWarning() {
        List<String> warnings = new ArrayList<>();
        HighlightAnalyzers.resolve(List.of(textField("title", "my_index_analyzer")), null, TEST_ANALYSIS_REGISTRY, warnings::add);
        assertThat(warnings, hasSize(1));
        assertThat(warnings.get(0), containsString("HIGHLIGHT on [title] falls back to [standard]"));
        assertThat(warnings.get(0), containsString("analyzer [my_index_analyzer]"));
        assertThat(warnings.get(0), containsString("WITH {\"analyzer\": <registered analyzer>}"));
    }

    // Two indices disagree on the analyzer. TextEsField.unknownAnalyzer is CONFLICT and HighlightAnalyzers warns.
    public void testMultiIndexAnalyzerConflictFallsBackAndWarns() {
        List<String> warnings = new ArrayList<>();
        Map<String, NamedAnalyzer> resolved = HighlightAnalyzers.resolve(
            List.of(unknownAnalyzerField(TextEsField.UnknownAnalyzer.CONFLICT)),
            null,
            TEST_ANALYSIS_REGISTRY,
            warnings::add
        );
        assertThat(resolved.get("title").name(), equalTo("standard"));
        assertThat(warnings, hasItem(containsString("indices disagree on the analyzer")));
    }

    // The mapping analyzer is bound under index.analysis, so the shard withheld its name. Standard is the only
    // thing HIGHLIGHT can use, but it has to say so rather than differ from what matched in silence.
    public void testIndexLocalAnalyzerFallsBackAndWarns() {
        List<String> warnings = new ArrayList<>();
        Map<String, NamedAnalyzer> resolved = HighlightAnalyzers.resolve(
            List.of(unknownAnalyzerField(TextEsField.UnknownAnalyzer.INDEX_LOCAL)),
            null,
            TEST_ANALYSIS_REGISTRY,
            warnings::add
        );
        assertThat(resolved.get("title").name(), equalTo("standard"));
        assertThat(warnings, hasItem(containsString("its analyzer is defined in the index settings")));
    }

    // WITH takes precedence; a mapping analyzer HIGHLIGHT cannot use must not warn once the user has set WITH.
    public void testWithAnalyzerSuppressesUnknownAnalyzerWarning() {
        for (var unknown : List.of(TextEsField.UnknownAnalyzer.CONFLICT, TextEsField.UnknownAnalyzer.INDEX_LOCAL)) {
            List<String> warnings = new ArrayList<>();
            HighlightAnalyzers.resolve(List.of(unknownAnalyzerField(unknown)), "keyword", TEST_ANALYSIS_REGISTRY, warnings::add);
            assertThat(warnings, hasSize(0));
        }
    }

    /** A {@code title} field whose analyzer name never reached the coordinator, for the given reason. */
    private static FieldAttribute unknownAnalyzerField(TextEsField.UnknownAnalyzer unknown) {
        return textField("title", null, TextEsField.DEFAULT_POSITION_INCREMENT_GAP, unknown);
    }

    public void testUnknownCommandAndDeclaredAnalyzersThrow() {
        expectThrows(
            InvalidArgumentException.class,
            () -> HighlightAnalyzers.resolve(List.of(textField("title", "whitespace")), "nope", TEST_ANALYSIS_REGISTRY)
        );
        expectThrows(InvalidArgumentException.class, () -> names(declaredField("note", "nope")));
    }

    private static List<String> names(NamedExpression... onFields) {
        return HighlightAnalyzers.resolve(List.of(onFields), null, TEST_ANALYSIS_REGISTRY)
            .values()
            .stream()
            .map(NamedAnalyzer::name)
            .toList();
    }

    private static FieldAttribute textField(String name, String analyzerName) {
        return textField(name, analyzerName, TextEsField.DEFAULT_POSITION_INCREMENT_GAP);
    }

    private static FieldAttribute textField(String name, String analyzerName, int positionIncrementGap) {
        return textField(name, analyzerName, positionIncrementGap, TextEsField.UnknownAnalyzer.NONE);
    }

    private static FieldAttribute textField(String name, String analyzerName, int gap, TextEsField.UnknownAnalyzer unknown) {
        return new FieldAttribute(
            EMPTY,
            name,
            new TextEsField(name, Map.of(), false, false, EsField.TimeSeriesFieldType.NONE, analyzerName, gap, unknown)
        );
    }

    private static ReferenceAttribute declaredField(String name, String analyzerName) {
        return new ReferenceAttribute(EMPTY, null, name, TEXT, Nullability.TRUE, null, false, analyzerName);
    }
}
