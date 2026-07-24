/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.type.CompactMultiTypeEsField;

/**
 * Analyzer behavior for cross-index and partially mapped field resolution.
 */
public class AnalyzerUnmappedMultiIndexGoldenTests extends AnalyzerUnmappedGoldenTestCase {

    private static void requireMatchOperator() {
        assumeTrue("Requires match operator", EsqlCapabilities.Cap.MATCH_OPERATOR_COLON.isEnabled());
    }

    private static void requireMatchFunction() {
        assumeTrue("Requires MATCH_FUNCTION", EsqlCapabilities.Cap.MATCH_FUNCTION.isEnabled());
    }

    private static void requireMatchPhraseFunction() {
        assumeTrue("Requires MATCH_PHRASE_FUNCTION", EsqlCapabilities.Cap.MATCH_PHRASE_FUNCTION.isEnabled());
    }

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public AnalyzerUnmappedMultiIndexGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    public void testPartiallyMappedFieldNullify() throws Exception {
        builder(nullify("""
            FROM sample_data, partial_mapping_sample_data
            | KEEP @timestamp, message, unmapped_message
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testPartiallyMappedField").nestedPath("nullify").run();
    }

    public void testPartiallyMappedFieldLoad() throws Exception {
        builder(load("""
            FROM sample_data, partial_mapping_sample_data
            | KEEP @timestamp, message, unmapped_message
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testPartiallyMappedField").nestedPath("load").run();
    }

    public void testMappedInOneIndexOnlyNullify() throws Exception {
        builder(nullify("""
            FROM sample_data, no_mapping_sample_data
            | KEEP message
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testMappedInOneIndexOnly").nestedPath("nullify").run();
    }

    public void testMappedInOneIndexOnlyLoad() throws Exception {
        builder(load("""
            FROM sample_data, no_mapping_sample_data
            | KEEP message
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testMappedInOneIndexOnly")
            .nestedPath("load")
            .expectationChangesAt(COMPACT_MULTI_TYPE_ES_FIELD)
            .run();
    }

    public void testMappedInOneIndexOnlyCastNullify() throws Exception {
        builder(nullify("""
            FROM sample_data, no_mapping_sample_data
            | EVAL x = message :: LONG
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testMappedInOneIndexOnlyCast").nestedPath("nullify").run();
    }

    public void testMappedInOneIndexOnlyCastLoad() throws Exception {
        builder(load("""
            FROM sample_data, no_mapping_sample_data
            | EVAL x = message :: LONG
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testMappedInOneIndexOnlyCast")
            .nestedPath("load")
            .expectationChangesAt(COMPACT_MULTI_TYPE_ES_FIELD)
            .run();
    }

    // message is keyword-mapped in sample_data and unmapped (loaded as keyword) in no_mapping_sample_data, so it
    // surfaces as a PotentiallyUnmappedKeywordEsField rather than a two-legged PUNK requiring a type conversion.
    // TO_TEXT is applied directly to that field attribute, so no keyword->keyword auto-cast happens.
    // See also testSingleTypeTextUnmappedToText.
    public void testMappedInOneIndexOnlyToTextNullify() throws Exception {
        builder(nullify("""
            FROM sample_data, no_mapping_sample_data
            | EVAL message_text = TO_TEXT(message)
            | KEEP message_text
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testMappedInOneIndexOnlyToText")
            .nestedPath("nullify")
            .since(CompactMultiTypeEsField.CompactMultiTypeEsField)
            .run();
    }

    public void testMappedInOneIndexOnlyToTextLoad() throws Exception {
        builder(load("""
            FROM sample_data, no_mapping_sample_data
            | EVAL message_text = TO_TEXT(message)
            | KEEP message_text
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testMappedInOneIndexOnlyToText")
            .nestedPath("load")
            .since(CompactMultiTypeEsField.CompactMultiTypeEsField)
            .run();
    }

    public void testMappedToNonKeywordInOneIndexOnlyNullify() throws Exception {
        builder(nullify("""
            FROM sample_data, no_mapping_sample_data
            | KEEP event_duration
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testMappedToNonKeywordInOneIndexOnly").nestedPath("nullify").run();
    }

    public void testMappedToNonKeywordInOneIndexOnlyLoad() throws Exception {
        builder(load("""
            FROM sample_data, no_mapping_sample_data
            | KEEP event_duration
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testMappedToNonKeywordInOneIndexOnly")
            .nestedPath("load")
            .expectationChangesAt(COMPACT_MULTI_TYPE_ES_FIELD)
            .run();
    }

    public void testTypeConflictMappedAndUnmappedWithCastNullify() throws Exception {
        builder(nullify("""
            FROM sample_data, no_mapping_sample_data
            | EVAL event_duration = event_duration::long
            | KEEP event_duration
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testTypeConflictMappedAndUnmappedWithCast")
            .nestedPath("nullify")
            .run();
    }

    public void testTypeConflictMappedAndUnmappedWithCastLoad() throws Exception {
        builder(load("""
            FROM sample_data, no_mapping_sample_data
            | EVAL event_duration = event_duration::long
            | KEEP event_duration
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testTypeConflictMappedAndUnmappedWithCast")
            .nestedPath("load")
            .expectationChangesAt(COMPACT_MULTI_TYPE_ES_FIELD)
            .run();
    }

    // @timestamp is mapped with conflicting types across sample_data_ts_long and sample_data, and unmapped in
    // no_mapping_sample_data. Unlike a plain two-legged PUNK, this is a genuine mapped-vs-mapped union type
    // conflict, so NULLIFY also builds a MultiTypeEsField/CompactMultiTypeEsField and needs the version split too.
    public void testTypeConflictMappedTimesTwoAndUnmappedNullify() throws Exception {
        builder(nullify("""
            FROM sample_data_ts_long, sample_data, no_mapping_sample_data
            | EVAL ts = @timestamp::date
            | KEEP ts
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testTypeConflictMappedTimesTwoAndUnmapped")
            .nestedPath("nullify")
            .expectationChangesAt(COMPACT_MULTI_TYPE_ES_FIELD)
            .run();
    }

    public void testTypeConflictMappedTimesTwoAndUnmappedLoad() throws Exception {
        builder(load("""
            FROM sample_data_ts_long, sample_data, no_mapping_sample_data
            | EVAL ts = @timestamp::date
            | KEEP ts
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testTypeConflictMappedTimesTwoAndUnmapped")
            .nestedPath("load")
            .expectationChangesAt(COMPACT_MULTI_TYPE_ES_FIELD)
            .run();
    }

    public void testNoTypeConflictKeywordAndUnmappedWhereNullify() throws Exception {
        builder(nullify("""
            FROM sample_data, no_mapping_sample_data
            | WHERE message::keyword LIKE "Connected*"
            | KEEP message
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testNoTypeConflictKeywordAndUnmappedWhere")
            .nestedPath("nullify")
            .run();
    }

    public void testNoTypeConflictKeywordAndUnmappedWhereLoad() throws Exception {
        builder(load("""
            FROM sample_data, no_mapping_sample_data
            | WHERE message::keyword LIKE "Connected*"
            | KEEP message
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testNoTypeConflictKeywordAndUnmappedWhere")
            .nestedPath("load")
            .expectationChangesAt(COMPACT_MULTI_TYPE_ES_FIELD)
            .run();
    }

    // All fields are partially unmapped (no_mapping_sample_data has no mapped fields).
    // Keyword fields should become PotentiallyUnmappedKeywordEsField; non-keyword fields should become InvalidMappedField.
    // No explicit field reference — all fields come from the implicit output of FROM.
    public void testPartiallyMappedFieldsAutomaticallyFoundNullify() throws Exception {
        builder(nullify("""
            FROM sample_data, no_mapping_sample_data
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testPartiallyMappedFieldsAutomaticallyFound")
            .nestedPath("nullify")
            .run();
    }

    public void testPartiallyMappedFieldsAutomaticallyFoundLoad() throws Exception {
        builder(load("""
            FROM sample_data, no_mapping_sample_data
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testPartiallyMappedFieldsAutomaticallyFound")
            .nestedPath("load")
            .expectationChangesAt(COMPACT_MULTI_TYPE_ES_FIELD)
            .run();
    }

    // Same as testPartiallyMappedFieldsAutomaticallyFound, but with an explicit KEEP * to verify wildcard expansion
    // handles partially-mapped fields correctly.
    public void testPartiallyMappedFieldsAutomaticallyFoundKeepStarNullify() throws Exception {
        builder(nullify("""
            FROM sample_data, no_mapping_sample_data
            | KEEP *
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testPartiallyMappedFieldsAutomaticallyFoundKeepStar")
            .nestedPath("nullify")
            .run();
    }

    public void testPartiallyMappedFieldsAutomaticallyFoundKeepStarLoad() throws Exception {
        builder(load("""
            FROM sample_data, no_mapping_sample_data
            | KEEP *
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testPartiallyMappedFieldsAutomaticallyFoundKeepStar")
            .nestedPath("load")
            .expectationChangesAt(COMPACT_MULTI_TYPE_ES_FIELD)
            .run();
    }

    public void testPartiallyMappedNonKeywordFieldMarkedAsPotentiallyUnmappedNullify() throws Exception {
        builder(nullify("""
            FROM sample_data, no_mapping_sample_data
            | KEEP @timestamp, event_duration
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testPartiallyMappedNonKeywordFieldMarkedAsPotentiallyUnmapped")
            .nestedPath("nullify")
            .run();
    }

    public void testPartiallyMappedNonKeywordFieldMarkedAsPotentiallyUnmappedLoad() throws Exception {
        builder(load("""
            FROM sample_data, no_mapping_sample_data
            | KEEP @timestamp, event_duration
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testPartiallyMappedNonKeywordFieldMarkedAsPotentiallyUnmapped")
            .nestedPath("load")
            .expectationChangesAt(COMPACT_MULTI_TYPE_ES_FIELD)
            .run();
    }

    public void testSingleTypeTextUnmappedNoCastLoad() throws Exception {
        builder(load("""
            FROM text_state_mapped, text_state_unmapped
            | KEEP txt
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSingleTypeTextUnmappedNoCastLoadOnly").nestedPath("load").run();
    }

    // The 'txt' field is text-mapped in text_state_mapped and unmapped (loaded as keyword) in text_state_unmapped.
    // TEXT is excluded from TYPE_TO_CONVERTER_FUNCTION, so ResolveTwoLeggedPunksInEsRelation cannot auto-cast the
    // unmapped leg to TEXT ahead of time. TO_TEXT is instead fused directly onto the raw values of both legs,
    // exactly as for the keyword case in testMappedInOneIndexOnlyToText.
    // https://github.com/elastic/elasticsearch/pull/153015#discussion_r3544806310.
    public void testSingleTypeTextUnmappedToTextNullify() throws Exception {
        builder(nullify("""
            FROM text_state_mapped, text_state_unmapped
            | EVAL txt_text = TO_TEXT(txt)
            | KEEP txt_text
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSingleTypeTextUnmappedToText")
            .nestedPath("nullify")
            .since(CompactMultiTypeEsField.CompactMultiTypeEsField)
            .run();
    }

    public void testSingleTypeTextUnmappedToTextLoad() throws Exception {
        builder(load("""
            FROM text_state_mapped, text_state_unmapped
            | EVAL txt_text = TO_TEXT(txt)
            | KEEP txt_text
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSingleTypeTextUnmappedToText")
            .nestedPath("load")
            .since(CompactMultiTypeEsField.CompactMultiTypeEsField)
            .run();
    }

    public void testSingleTypeDenseVectorUnmappedNoCastLoad() throws Exception {
        builder(load("""
            FROM dense_vector, dense_vector_unmapped
            | KEEP float_vector
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSingleTypeDenseVectorUnmappedNoCastLoadOnly")
            .nestedPath("load")
            .run();
    }

    public void testSingleTypeAggregateMetricDoubleUnmappedNoCastLoad() throws Exception {
        builder(load("""
            FROM k8s-downsampled, k8s_unmapped
            | KEEP network.eth0.tx
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSingleTypeAggregateMetricDoubleUnmappedNoCastLoadOnly")
            .nestedPath("load")
            .since(CompactMultiTypeEsField.CompactMultiTypeEsField)
            .run();
    }

    public void testSingleTypeTextUnmappedWithMatchOperatorLoad() throws Exception {
        requireMatchOperator();
        builder(load("""
            FROM text_state_mapped, text_state_unmapped
            | WHERE txt:"Faulkner"
            | KEEP txt
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSingleTypeTextUnmappedWithMatchOperatorLoadOnly")
            .nestedPath("load")
            .run();
    }

    public void testSingleTypeTextUnmappedWithMatchFunctionLoad() throws Exception {
        requireMatchFunction();
        builder(load("""
            FROM text_state_mapped, text_state_unmapped
            | WHERE match(txt, "Faulkner")
            | KEEP txt
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSingleTypeTextUnmappedWithMatchFunctionLoadOnly")
            .nestedPath("load")
            .run();
    }

    public void testSingleTypeTextMappedUnmappedAndNonExistentWithMatchFunctionLoad() throws Exception {
        requireMatchFunction();
        builder(load("""
            FROM text_state_mapped, text_state_unmapped, text_state_nonexistent
            | WHERE match(txt, "Faulkner") OR txt IS NULL
            | KEEP txt
            """)).existingGoldenPath(
            "AnalyzerUnmappedGoldenTests",
            "testSingleTypeTextMappedUnmappedAndNonExistentWithMatchFunctionLoadOnly"
        ).nestedPath("load").run();
    }

    public void testSingleTypeTextMappedUnmappedAndNonExistentWithMatchFunctionAndMetadataKeepLoad() throws Exception {
        requireMatchFunction();
        builder(load("""
            FROM text_state_mapped, text_state_unmapped, text_state_nonexistent METADATA _index
            | WHERE match(txt, "Faulkner") OR txt IS NULL
            | KEEP _index, doc_id, txt
            | SORT _index
            """)).existingGoldenPath(
            "AnalyzerUnmappedGoldenTests",
            "testSingleTypeTextMappedUnmappedAndNonExistentWithMatchFunctionAndMetadataKeepLoadOnly"
        ).nestedPath("load").run();
    }

    public void testSingleTypeTextUnmappedWithMatchPhraseFunctionLoad() throws Exception {
        requireMatchPhraseFunction();
        builder(load("""
            FROM text_state_mapped, text_state_unmapped
            | WHERE match_phrase(txt, "William Faulkner")
            | KEEP txt
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testSingleTypeTextUnmappedWithMatchPhraseFunctionLoadOnly")
            .nestedPath("load")
            .run();
    }

    // first_name and last_name are keyword, partially unmapped (missing in employees_no_names).
    // They should appear as PotentiallyUnmappedKeywordEsField in the EsRelation without being explicitly referenced.
    public void testPartiallyMappedKeywordFieldLoadedWithoutExplicitReferenceNullify() throws Exception {
        builder(nullify("""
            FROM employees, employees_no_names
            | SORT emp_no
            | LIMIT 1
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testPartiallyMappedKeywordFieldLoadedWithoutExplicitReference")
            .nestedPath("nullify")
            .run();
    }

    public void testPartiallyMappedKeywordFieldLoadedWithoutExplicitReferenceLoad() throws Exception {
        builder(load("""
            FROM employees, employees_no_names
            | SORT emp_no
            | LIMIT 1
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testPartiallyMappedKeywordFieldLoadedWithoutExplicitReference")
            .nestedPath("load")
            .run();
    }

    // first_name (keyword, partially unmapped) should become PotentiallyUnmappedKeywordEsField.
    // gender (keyword, fully mapped in both indices) should remain a regular KeywordEsField.
    public void testNonPartiallyMappedKeywordFieldNotLoadedFromSourceNullify() throws Exception {
        builder(nullify("""
            FROM employees, employees_no_names
            | KEEP first_name, gender
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testNonPartiallyMappedKeywordFieldNotLoadedFromSource")
            .nestedPath("nullify")
            .run();
    }

    public void testNonPartiallyMappedKeywordFieldNotLoadedFromSourceLoad() throws Exception {
        builder(load("""
            FROM employees, employees_no_names
            | KEEP first_name, gender
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testNonPartiallyMappedKeywordFieldNotLoadedFromSource")
            .nestedPath("load")
            .run();
    }

    // gender is text in employees_gender_text but missing in employees_no_gender.
    // It should appear as InvalidMappedField (unsupported) in the EsRelation.
    public void testPartiallyMappedTextFieldMarkedAsPotentiallyUnmappedNullify() throws Exception {
        builder(nullify("""
            FROM employees_gender_text, employees_no_gender
            | KEEP gender
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testPartiallyMappedTextFieldMarkedAsPotentiallyUnmapped")
            .nestedPath("nullify")
            .run();
    }

    public void testPartiallyMappedTextFieldMarkedAsPotentiallyUnmappedLoad() throws Exception {
        builder(load("""
            FROM employees_gender_text, employees_no_gender
            | KEEP gender
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testPartiallyMappedTextFieldMarkedAsPotentiallyUnmapped")
            .nestedPath("load")
            .run();
    }

    // DROP a single partially-mapped keyword field (message), leaving only non-keyword fields.
    public void testPartiallyMappedFieldsDropOnePartiallyMappedNullify() throws Exception {
        builder(nullify("""
            FROM sample_data, no_mapping_sample_data
            | DROP message
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testPartiallyMappedFieldsDropOnePartiallyMapped")
            .nestedPath("nullify")
            .run();
    }

    public void testPartiallyMappedFieldsDropOnePartiallyMappedLoad() throws Exception {
        builder(load("""
            FROM sample_data, no_mapping_sample_data
            | DROP message
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testPartiallyMappedFieldsDropOnePartiallyMapped")
            .nestedPath("load")
            .expectationChangesAt(COMPACT_MULTI_TYPE_ES_FIELD)
            .run();
    }

    // DROP a single partially-mapped non-keyword field (event_duration), leaving message and the other non-keyword fields.
    public void testPartiallyMappedFieldsDropOnePartiallyMappedNonKeywordNullify() throws Exception {
        builder(nullify("""
            FROM sample_data, no_mapping_sample_data
            | DROP event_duration
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testPartiallyMappedFieldsDropOnePartiallyMappedNonKeyword")
            .nestedPath("nullify")
            .run();
    }

    public void testPartiallyMappedFieldsDropOnePartiallyMappedNonKeywordLoad() throws Exception {
        builder(load("""
            FROM sample_data, no_mapping_sample_data
            | DROP event_duration
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testPartiallyMappedFieldsDropOnePartiallyMappedNonKeyword")
            .nestedPath("load")
            .expectationChangesAt(COMPACT_MULTI_TYPE_ES_FIELD)
            .run();
    }

    // DROP with wildcards on partially-mapped non-keyword fields, leaving only the keyword field (message).
    public void testPartiallyMappedFieldsDropNonKeywordWithWildcardsNullify() throws Exception {
        builder(nullify("""
            FROM sample_data, no_mapping_sample_data
            | DROP *_ip, *_duration, @timestamp
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testPartiallyMappedFieldsDropNonKeywordWithWildcards")
            .nestedPath("nullify")
            .run();
    }

    public void testPartiallyMappedFieldsDropNonKeywordWithWildcardsLoad() throws Exception {
        builder(load("""
            FROM sample_data, no_mapping_sample_data
            | DROP *_ip, *_duration, @timestamp
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testPartiallyMappedFieldsDropNonKeywordWithWildcards")
            .nestedPath("load")
            .expectationChangesAt(COMPACT_MULTI_TYPE_ES_FIELD)
            .run();
    }

    // DROP with wildcards on partially-mapped keyword fields, leaving only a few non-keyword fields.
    public void testPartiallyMappedFieldsDropKeywordWithWildcardsNullify() throws Exception {
        builder(nullify("""
            FROM employees, employees_no_names
            | DROP *date*, gender, height*, languages*, *_hired, *_seconds, *_positions, salary_change*
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testPartiallyMappedFieldsDropKeywordWithWildcards")
            .nestedPath("nullify")
            .run();
    }

    public void testPartiallyMappedFieldsDropKeywordWithWildcardsLoad() throws Exception {
        builder(load("""
            FROM employees, employees_no_names
            | DROP *date*, gender, height*, languages*, *_hired, *_seconds, *_positions, salary_change*
            """)).existingGoldenPath("AnalyzerUnmappedGoldenTests", "testPartiallyMappedFieldsDropKeywordWithWildcards")
            .nestedPath("load")
            .run();
    }
}
