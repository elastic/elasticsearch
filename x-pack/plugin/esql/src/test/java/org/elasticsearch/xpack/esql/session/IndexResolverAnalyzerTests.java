/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilitiesBuilder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.IndexAnalyzerGroup;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.core.type.TextEsField.UnknownAnalyzer;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class IndexResolverAnalyzerTests extends ESTestCase {

    /**
     * Shared analyzer is kept only when every index reports the same name and
     * {@code position_increment_gap}. A disagreement or a missing name returns null.
     * HIGHLIGHT treats that as {@code standard}, and warns whenever the null came from a disagreement.
     */
    public void testSharedIndexAnalyzerNeedsEveryIndexToAgree() {
        assertAnalyzer(resolveTitle("english", "english"), "english", UnknownAnalyzer.NONE);
        assertAnalyzer(resolveTitle("english", "standard"), null, UnknownAnalyzer.CONFLICT);
        // A node new enough to run HIGHLIGHT always names a text field's analyzer, so a silent index is one whose
        // index-local name was withheld. That is a real disagreement with an index naming a built-in.
        assertAnalyzer(resolveTitle("english", null), null, UnknownAnalyzer.CONFLICT);
        // Every index silent with nothing withheld: no analyzer to speak of, so standard without a warning.
        assertAnalyzer(resolveTitle(null, null), null, UnknownAnalyzer.NONE);

        TextEsField sameGap = resolveTitle(index("idx-a", "english", 0), index("idx-b", "english", 0));
        assertAnalyzer(sameGap, "english", UnknownAnalyzer.NONE);
        assertThat(sameGap.positionIncrementGap(), equalTo(0));
        // Same name, different gap: the analyzers behave differently on multi-value fields.
        assertAnalyzer(resolveTitle(index("idx-a", "english", 0), index("idx-b", "english", 100)), null, UnknownAnalyzer.CONFLICT);
    }

    /**
     * When every index withheld an {@code index.analysis} name there is no disagreement, but HIGHLIGHT still cannot
     * rebuild the analyzer, so the reason has to survive the merge. Mixing a withheld name with a reported one is a
     * disagreement like any other.
     */
    public void testWithheldIndexLocalAnalyzerSurvivesTheMerge() {
        assertAnalyzer(resolveTitle(indexLocal("idx-a"), indexLocal("idx-b")), null, UnknownAnalyzer.INDEX_LOCAL);
        assertAnalyzer(resolveTitle(indexLocal("idx-a"), index("idx-b", "english", 100)), null, UnknownAnalyzer.CONFLICT);
    }

    /**
     * A disagreement keeps which indices use which analyzer, so HIGHLIGHT can pick per row. Indices are found by
     * walking every index response: the merge itself only sees one response per mapping hash, and here
     * {@code idx-b} and {@code idx-c} share one. A withheld name and a name an older node did not report are groups
     * of their own.
     */
    public void testConflictKeepsAnalyzerGroups() {
        TextEsField field = resolveTitle(
            index("idx-a", "english", 0),
            index("idx-b", "standard", 100, "shared"),
            index("idx-c", "standard", 100, "shared"),
            indexLocal("idx-d"),
            index("idx-e", null, TextEsField.DEFAULT_POSITION_INCREMENT_GAP)
        );
        assertAnalyzer(field, null, UnknownAnalyzer.CONFLICT);
        assertThat(
            field.analyzerGroups(),
            contains(
                new IndexAnalyzerGroup("english", false, 0, Set.of("idx-a")),
                new IndexAnalyzerGroup("standard", false, 100, Set.of("idx-b", "idx-c")),
                new IndexAnalyzerGroup(null, true, TextEsField.DEFAULT_POSITION_INCREMENT_GAP, Set.of("idx-d")),
                new IndexAnalyzerGroup(null, false, TextEsField.DEFAULT_POSITION_INCREMENT_GAP, Set.of("idx-e"))
            )
        );
    }

    /** Only a HIGHLIGHT reads the groups, so without one a disagreement records just the conflict. */
    public void testConflictSkipsAnalyzerGroupsWhenNotNeeded() {
        TextEsField field = resolveTitle(false, index("idx-a", "english", 0), index("idx-b", "standard", 100));
        assertThat(field.unknownAnalyzer(), equalTo(UnknownAnalyzer.CONFLICT));
        assertThat(field.analyzerGroups(), nullValue());
    }

    private static void assertAnalyzer(TextEsField field, String analyzerName, UnknownAnalyzer unknownAnalyzer) {
        Matcher<String> nameMatcher = analyzerName == null ? nullValue(String.class) : equalTo(analyzerName);
        assertThat(field.analyzerName(), nameMatcher);
        assertThat(field.unknownAnalyzer(), equalTo(unknownAnalyzer));
        assertThat(field.analyzerGroups() != null, equalTo(unknownAnalyzer == UnknownAnalyzer.CONFLICT));
    }

    private static TextEsField resolveTitle(String first, String second) {
        return resolveTitle(
            index("idx-a", first, TextEsField.DEFAULT_POSITION_INCREMENT_GAP),
            index("idx-b", second, TextEsField.DEFAULT_POSITION_INCREMENT_GAP)
        );
    }

    private static TextEsField resolveTitle(FieldCapabilitiesIndexResponse... indices) {
        return resolveTitle(true, indices);
    }

    private static TextEsField resolveTitle(boolean needsAnalyzerGroups, FieldCapabilitiesIndexResponse... indices) {
        FieldCapabilitiesResponse caps = FieldCapabilitiesResponse.builder().withIndexResponses(List.of(indices)).build();
        IndexResolution resolution = IndexResolver.mergedMappings(
            "idx-*",
            false,
            new IndexResolver.FieldsInfo(caps, TransportVersion.current(), false, false, false, false, needsAnalyzerGroups, false),
            false,
            IndexResolver.DO_NOT_GROUP
        );
        var esField = resolution.get().mapping().get("title");
        assertThat(esField, instanceOf(TextEsField.class));
        return (TextEsField) esField;
    }

    /** Each index has its own mapping hash, so the merge compares every index with the first. */
    private static FieldCapabilitiesIndexResponse index(String index, String analyzer, int positionIncrementGap) {
        return index(index, analyzer, positionIncrementGap, index);
    }

    /** Indices with the same {@code mappingHash} share one response in the merge. */
    private static FieldCapabilitiesIndexResponse index(String index, String analyzer, int positionIncrementGap, String mappingHash) {
        var title = new IndexFieldCapabilitiesBuilder("title", "text").indexAnalyzer(analyzer)
            .indexAnalyzerPositionIncrementGap(positionIncrementGap)
            .build();
        return new FieldCapabilitiesIndexResponse(index, mappingHash, Map.of("title", title), true, IndexMode.STANDARD);
    }

    /** An index that analyzes {@code title} with an {@code index.analysis} name, so it reports no name at all. */
    private static FieldCapabilitiesIndexResponse indexLocal(String index) {
        var title = new IndexFieldCapabilitiesBuilder("title", "text").indexLocalAnalyzer(true).build();
        return new FieldCapabilitiesIndexResponse(index, index, Map.of("title", title), true, IndexMode.STANDARD);
    }
}
