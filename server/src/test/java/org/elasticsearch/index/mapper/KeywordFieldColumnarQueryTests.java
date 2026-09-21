/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.common.unit.Fuzziness;

import java.io.IOException;

public class KeywordFieldColumnarQueryTests extends AbstractColumnarBinaryLayoutTestCase {

    @Override
    protected String fieldTypeName() {
        return "keyword";
    }

    @Override
    protected BinaryDocValuesFormat binaryFormatOf(MappedFieldType fieldType) {
        return ((KeywordFieldMapper.KeywordFieldType) fieldType).binaryFormat();
    }

    /** The queries a keyword field answers from its doc values on top of the ones every field type here does. */
    public void testTheRestOfTheKeywordQueriesAreAnsweredFromTheColumn() throws IOException {
        forEachLayout((field, context, hits) -> {
            hits.assertMatches(
                ((StringFieldType) field).rangeQuery("alpha", "delta", true, false, context),
                value -> value.compareTo("alpha") >= 0 && value.compareTo("delta") < 0
            );
            hits.assertMatches(field.fuzzyQuery("alpho", Fuzziness.ONE, 0, 50, true, context, null), value -> value.equals("alpha"));
            hits.assertMatches(field.termQueryCaseInsensitive("ALPHA", context), "alpha"::equals);
            final Automaton automaton = Automata.makeString("gamma");
            hits.assertMatches(
                field.automatonQuery(() -> automaton, () -> new CharacterRunAutomaton(automaton), null, context, "gamma"),
                "gamma"::equals
            );
        });
    }
}
