/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.compiler;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.CharCodes;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.TimestampComponentType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.BitmaskRegistry;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.MultiTokenType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.SubTokenDelimiterCharParsingInfo;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.SubTokenEvaluator;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.SubTokenType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.SubstringView;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.TimestampFormat;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.TokenType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.MultiTokenFormat;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.PatternUtils;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.Schema;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.EqualsStringConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.LengthStringConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.NotEqualsStringConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.StringConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.StringSetConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.StringToIntMapConstraint;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SchemaCompilerTests extends ESTestCase {

    public void testCreateSubTokenEvaluatorEqualsConstraint() {
        StringConstraint constraint = new EqualsStringConstraint("test");
        SubTokenEvaluator<SubstringView> evaluator = SubTokenEvaluatorFactory.from(0x01, constraint);

        assertEquals(0x01, evaluator.bitmask);
        assertTrue(evaluator.evaluate(new SubstringView("test")) >= 0);
        assertTrue(evaluator.evaluate(new SubstringView("other")) < 0);
    }

    public void testCreateSubTokenEvaluatorNotEqualsConstraint() {
        StringConstraint constraint = new NotEqualsStringConstraint("test");
        SubTokenEvaluator<SubstringView> evaluator = SubTokenEvaluatorFactory.from(0x02, constraint);

        assertEquals(0x02, evaluator.bitmask);
        assertTrue(evaluator.evaluate(new SubstringView("other")) >= 0);
        assertTrue(evaluator.evaluate(new SubstringView("test")) < 0);
    }

    public void testCreateSubTokenEvaluatorSetConstraint() {
        StringConstraint constraint = new StringSetConstraint(Set.of("One", "Two", "Three"));
        SubTokenEvaluator<SubstringView> evaluator = SubTokenEvaluatorFactory.from(0x04, constraint);

        assertEquals(0x04, evaluator.bitmask);
        assertEquals(1, evaluator.evaluate(new SubstringView("One")));
        assertEquals(1, evaluator.evaluate(new SubstringView("Two")));
        assertEquals(1, evaluator.evaluate(new SubstringView("Three")));
        assertTrue(evaluator.evaluate(new SubstringView("Four")) < 0);
    }

    public void testCreateSubTokenEvaluatorMapConstraint() {
        StringConstraint constraint = new StringToIntMapConstraint(Map.of("One", 1, "Two", 2, "Three", 3));
        SubTokenEvaluator<SubstringView> evaluator = SubTokenEvaluatorFactory.from(0x08, constraint);

        assertEquals(0x08, evaluator.bitmask);
        assertEquals(1, evaluator.evaluate(new SubstringView("One")));
        assertEquals(2, evaluator.evaluate(new SubstringView("Two")));
        assertEquals(3, evaluator.evaluate(new SubstringView("Three")));
        assertTrue(evaluator.evaluate(new SubstringView("Four")) < 0);
    }

    public void testCreateSubTokenEvaluatorLengthConstraint() {
        StringConstraint constraint = new LengthStringConstraint(3);
        SubTokenEvaluator<SubstringView> evaluator = SubTokenEvaluatorFactory.from(0x08, constraint);

        assertEquals(0x08, evaluator.bitmask);
        assertTrue(evaluator.evaluate(new SubstringView("abc")) >= 0);
        assertTrue(evaluator.evaluate(new SubstringView("abcd")) < 0);
    }

    public void testCreateSubTokenEvaluatorAndConstraint() {
        StringConstraint constraint1 = new EqualsStringConstraint("test");
        StringConstraint constraint2 = new LengthStringConstraint(4);
        StringConstraint combined = constraint1.and(constraint2);

        SubTokenEvaluator<SubstringView> evaluator = SubTokenEvaluatorFactory.from(0x10, combined);

        assertEquals(0x10, evaluator.bitmask);
        assertTrue(evaluator.evaluate(new SubstringView("test")) >= 0);
        assertTrue(evaluator.evaluate(new SubstringView("testing")) < 0);
        assertTrue(evaluator.evaluate(new SubstringView("Test")) < 0);
    }

    public void testCreateSubTokenEvaluatorOrConstraint() {
        StringConstraint constraint1 = new EqualsStringConstraint("test");
        StringConstraint constraint2 = new EqualsStringConstraint("Test");
        StringConstraint combined = constraint1.or(constraint2);

        SubTokenEvaluator<SubstringView> evaluator = SubTokenEvaluatorFactory.from(0x20, combined);

        assertEquals(0x20, evaluator.bitmask);
        assertTrue(evaluator.evaluate(new SubstringView("test")) >= 0);
        assertTrue(evaluator.evaluate(new SubstringView("Test")) >= 0);
        assertTrue(evaluator.evaluate(new SubstringView("testing")) < 0);
    }

    public void testCreateSubTokenEvaluatorComplexAndOrConstraint() {
        StringConstraint constraint1 = new EqualsStringConstraint("One");
        StringConstraint constraint2 = new StringSetConstraint(Set.of("One", "Two", "Three"));
        StringConstraint constraint3 = new LengthStringConstraint(4);
        StringConstraint combined = constraint1.and(constraint2).or(constraint3);

        SubTokenEvaluator<SubstringView> evaluator = SubTokenEvaluatorFactory.from(0x40, combined);

        assertEquals(0x40, evaluator.bitmask);
        assertTrue(evaluator.evaluate(new SubstringView("One")) >= 0);
        assertTrue(evaluator.evaluate(new SubstringView("Four")) >= 0);
        assertTrue(evaluator.evaluate(new SubstringView("Two")) < 0);
        assertTrue(evaluator.evaluate(new SubstringView("Three")) < 0);
    }

    public void testMergeIntRangeBitmasks_OverlappingRanges() {
        ArrayList<SchemaCompiler.IntRangeBitmask> input = new ArrayList<>();
        input.add(SchemaCompiler.IntRangeBitmask.of(10, 20, 0x01));
        input.add(SchemaCompiler.IntRangeBitmask.of(15, 20, 0x02));
        input.add(SchemaCompiler.IntRangeBitmask.of(20, 30, 0x04));
        input.add(SchemaCompiler.IntRangeBitmask.of(25, 30, 0x08));

        ArrayList<SchemaCompiler.IntRangeBitmask> expected = new ArrayList<>();
        expected.add(SchemaCompiler.IntRangeBitmask.of(Integer.MIN_VALUE, 9, 0x00));
        expected.add(SchemaCompiler.IntRangeBitmask.of(10, 14, 0x01));
        expected.add(SchemaCompiler.IntRangeBitmask.of(15, 19, 0x03));
        expected.add(SchemaCompiler.IntRangeBitmask.of(20, 20, 0x07));
        expected.add(SchemaCompiler.IntRangeBitmask.of(21, 24, 0x04));
        expected.add(SchemaCompiler.IntRangeBitmask.of(25, 30, 0x0C));
        expected.add(SchemaCompiler.IntRangeBitmask.of(31, Integer.MAX_VALUE, 0x00));

        assertEquals(expected, SchemaCompiler.mergeIntRangeBitmasks(input));
    }

    public void testMergeIntRangeBitmasks_GapsBetweenRanges() {
        ArrayList<SchemaCompiler.IntRangeBitmask> input = new ArrayList<>();
        input.add(SchemaCompiler.IntRangeBitmask.of(10, 15, 0x01));
        input.add(SchemaCompiler.IntRangeBitmask.of(20, 25, 0x02));

        ArrayList<SchemaCompiler.IntRangeBitmask> expected = new ArrayList<>();
        expected.add(SchemaCompiler.IntRangeBitmask.of(Integer.MIN_VALUE, 9, 0x00));
        expected.add(SchemaCompiler.IntRangeBitmask.of(10, 15, 0x01));
        expected.add(SchemaCompiler.IntRangeBitmask.of(16, 19, 0x00));
        expected.add(SchemaCompiler.IntRangeBitmask.of(20, 25, 0x02));
        expected.add(SchemaCompiler.IntRangeBitmask.of(26, Integer.MAX_VALUE, 0x00));

        assertEquals(expected, SchemaCompiler.mergeIntRangeBitmasks(input));
    }

    public void testMergeIntRangeBitmasks_SingleIntegerRanges() {
        ArrayList<SchemaCompiler.IntRangeBitmask> input = new ArrayList<>();
        input.add(SchemaCompiler.IntRangeBitmask.of(10, 15, 0x01));
        input.add(SchemaCompiler.IntRangeBitmask.of(15, 15, 0x02));
        input.add(SchemaCompiler.IntRangeBitmask.of(0, 30, 0x04));

        ArrayList<SchemaCompiler.IntRangeBitmask> expected = new ArrayList<>();
        expected.add(SchemaCompiler.IntRangeBitmask.of(Integer.MIN_VALUE, -1, 0x00));
        expected.add(SchemaCompiler.IntRangeBitmask.of(0, 9, 0x04));
        expected.add(SchemaCompiler.IntRangeBitmask.of(10, 14, 0x05));
        expected.add(SchemaCompiler.IntRangeBitmask.of(15, 15, 0x07));
        expected.add(SchemaCompiler.IntRangeBitmask.of(16, 30, 0x04));
        expected.add(SchemaCompiler.IntRangeBitmask.of(31, Integer.MAX_VALUE, 0x00));

        assertEquals(expected, SchemaCompiler.mergeIntRangeBitmasks(input));
    }

    public void testMergeIntRangeBitmasks_IntegerMinMax() {
        ArrayList<SchemaCompiler.IntRangeBitmask> input = new ArrayList<>();
        input.add(SchemaCompiler.IntRangeBitmask.of(Integer.MIN_VALUE, 10, 0x01));
        input.add(SchemaCompiler.IntRangeBitmask.of(0, Integer.MAX_VALUE, 0x02));

        ArrayList<SchemaCompiler.IntRangeBitmask> expected = new ArrayList<>();
        expected.add(SchemaCompiler.IntRangeBitmask.of(Integer.MIN_VALUE, -1, 0x01));
        expected.add(SchemaCompiler.IntRangeBitmask.of(0, 10, 0x03));
        expected.add(SchemaCompiler.IntRangeBitmask.of(11, Integer.MAX_VALUE, 0x02));

        assertEquals(expected, SchemaCompiler.mergeIntRangeBitmasks(input));
    }

    public void testMergeIntRangeBitmasks_ComplexCase() {
        ArrayList<SchemaCompiler.IntRangeBitmask> input = new ArrayList<>();
        input.add(SchemaCompiler.IntRangeBitmask.of(10, 20, 0x01));
        input.add(SchemaCompiler.IntRangeBitmask.of(15, 25, 0x02));
        input.add(SchemaCompiler.IntRangeBitmask.of(20, 30, 0x04));
        input.add(SchemaCompiler.IntRangeBitmask.of(25, 35, 0x08));
        input.add(SchemaCompiler.IntRangeBitmask.of(22, 22, 0x10));
        input.add(SchemaCompiler.IntRangeBitmask.of(Integer.MIN_VALUE, 100, 0x20));
        input.add(SchemaCompiler.IntRangeBitmask.of(-50, -30, 0x40));
        input.add(SchemaCompiler.IntRangeBitmask.of(500, 2000, 0x80));

        ArrayList<SchemaCompiler.IntRangeBitmask> expected = new ArrayList<>();
        expected.add(SchemaCompiler.IntRangeBitmask.of(Integer.MIN_VALUE, -51, 0x20));
        expected.add(SchemaCompiler.IntRangeBitmask.of(-50, -30, 0x60));
        expected.add(SchemaCompiler.IntRangeBitmask.of(-29, 9, 0x20));
        expected.add(SchemaCompiler.IntRangeBitmask.of(10, 14, 0x21));
        expected.add(SchemaCompiler.IntRangeBitmask.of(15, 19, 0x23));
        expected.add(SchemaCompiler.IntRangeBitmask.of(20, 20, 0x27));
        expected.add(SchemaCompiler.IntRangeBitmask.of(21, 21, 0x26));
        expected.add(SchemaCompiler.IntRangeBitmask.of(22, 22, 0x36));
        expected.add(SchemaCompiler.IntRangeBitmask.of(23, 24, 0x26));
        expected.add(SchemaCompiler.IntRangeBitmask.of(25, 25, 0x2E));
        expected.add(SchemaCompiler.IntRangeBitmask.of(26, 30, 0x2C));
        expected.add(SchemaCompiler.IntRangeBitmask.of(31, 35, 0x28));
        expected.add(SchemaCompiler.IntRangeBitmask.of(36, 100, 0x20));
        expected.add(SchemaCompiler.IntRangeBitmask.of(101, 499, 0x00));
        expected.add(SchemaCompiler.IntRangeBitmask.of(500, 2000, 0x80));
        expected.add(SchemaCompiler.IntRangeBitmask.of(2001, Integer.MAX_VALUE, 0x00));

        assertExpectedIntRangeBitmaskLists(expected, SchemaCompiler.mergeIntRangeBitmasks(input));
    }

    private static void assertExpectedIntRangeBitmaskLists(
        ArrayList<SchemaCompiler.IntRangeBitmask> expected,
        ArrayList<SchemaCompiler.IntRangeBitmask> actual
    ) {
        if (expected.equals(actual)) {
            return;
        }
        for (int i = 0; i < expected.size(); i++) {
            SchemaCompiler.IntRangeBitmask expectedRange = expected.get(i);
            if (i < actual.size()) {
                SchemaCompiler.IntRangeBitmask actualRange = actual.get(i);
                assertEquals("Expected " + expectedRange + " at index " + i + ", but found " + actualRange, expectedRange, actualRange);
            } else {
                fail("Expected to find " + expectedRange + " at index " + i + ", but actual list is shorter.");
            }
        }
    }

    public void testSchemaCompilation() {
        Schema schema = Schema.getInstance();
        CompiledSchema compiledSchema = SchemaCompiler.compile(schema);

        assertNotNull(compiledSchema);

        BitmaskRegistry<SubTokenType> subTokenBitmaskRegistry = compiledSchema.subTokenBitmaskRegistry;

        subTokenBitmaskRegistry.getAllRegisteredTypes().forEach(subTokenType -> {
            try {
                subTokenType.getHigherLevelBitmaskByPosition(compiledSchema.maxSubTokensPerToken - 1);
            } catch (Exception e) {
                fail("Should not have thrown exception: " + e.getMessage());
            }
        });

        int int_bitmask = subTokenBitmaskRegistry.getBitmask("integer");
        int hex_bitmask = subTokenBitmaskRegistry.getBitmask("hex");
        int MM_bitmask = subTokenBitmaskRegistry.getBitmask("MM");
        int DD_bitmask = subTokenBitmaskRegistry.getBitmask("DD");
        int YYYY_bitmask = subTokenBitmaskRegistry.getBitmask("YYYY");
        int hh_bitmask = subTokenBitmaskRegistry.getBitmask("hh");
        int HH_bitmask = subTokenBitmaskRegistry.getBitmask("HH");
        int mm_bitmask = subTokenBitmaskRegistry.getBitmask("mm");
        int ss_bitmask = subTokenBitmaskRegistry.getBitmask("ss");
        int TZOhhmm_bitmask = subTokenBitmaskRegistry.getBitmask("TZOhhmm");
        int Mon_subToken_bitmask = subTokenBitmaskRegistry.getBitmask("Mon");
        int Day_bitmask = subTokenBitmaskRegistry.getBitmask("Day");
        int octet_bitmask = subTokenBitmaskRegistry.getBitmask("octet");

        int allIntegerSubTokenBitmask = compiledSchema.allIntegerSubTokenBitmask;
        assertNotEquals(0x00, allIntegerSubTokenBitmask & MM_bitmask);
        assertNotEquals(0x00, allIntegerSubTokenBitmask & DD_bitmask);
        assertNotEquals(0x00, allIntegerSubTokenBitmask & YYYY_bitmask);
        assertNotEquals(0x00, allIntegerSubTokenBitmask & hh_bitmask);
        assertNotEquals(0x00, allIntegerSubTokenBitmask & mm_bitmask);
        assertNotEquals(0x00, allIntegerSubTokenBitmask & ss_bitmask);
        assertNotEquals(0x00, allIntegerSubTokenBitmask & TZOhhmm_bitmask);
        assertEquals(0x00, allIntegerSubTokenBitmask & Mon_subToken_bitmask);
        assertEquals(0x00, allIntegerSubTokenBitmask & Day_bitmask);
        assertNotEquals(0x00, allIntegerSubTokenBitmask & octet_bitmask);

        assertEquals(int_bitmask, compiledSchema.intSubTokenBitmask);
        assertEquals(int_bitmask | hex_bitmask, compiledSchema.genericSubTokenTypesBitmask);

        int[] charToSubTokenBitmask = compiledSchema.charToSubTokenBitmask;
        int charSubTokenBitmask = charToSubTokenBitmask['M'];
        assertEquals(0x00, charSubTokenBitmask & MM_bitmask);
        assertNotEquals(0x00, charSubTokenBitmask & Mon_subToken_bitmask);
        assertEquals(0x00, charSubTokenBitmask & DD_bitmask);
        assertNotEquals(0x00, charSubTokenBitmask & Day_bitmask);
        assertEquals(0x00, charSubTokenBitmask & int_bitmask);
        assertEquals(0x00, charSubTokenBitmask & hex_bitmask);

        charSubTokenBitmask = charToSubTokenBitmask['3'];
        assertNotEquals(0x00, charSubTokenBitmask & MM_bitmask);
        assertEquals(0x00, charSubTokenBitmask & Mon_subToken_bitmask);
        assertNotEquals(0x00, charSubTokenBitmask & DD_bitmask);
        assertNotEquals(0x00, charSubTokenBitmask & int_bitmask);
        assertNotEquals(0x00, charSubTokenBitmask & hex_bitmask);
        assertEquals(0x00, charSubTokenBitmask & Day_bitmask);

        byte[] charToCharType = compiledSchema.charToCharType;
        assertEquals(CharCodes.ALPHABETIC_CHAR_CODE, charToCharType['M']);
        assertEquals(CharCodes.DIGIT_CHAR_CODE, charToCharType['3']);
        assertEquals(CharCodes.SUBTOKEN_DELIMITER_CHAR_CODE, charToCharType['/']);
        assertEquals(CharCodes.SUBTOKEN_DELIMITER_CHAR_CODE, charToCharType[':']);
        assertEquals(CharCodes.TOKEN_DELIMITER_CHAR_CODE, charToCharType[' ']);

        assertEquals(6, compiledSchema.maxSubTokensPerToken);
        assertEquals(5, compiledSchema.maxTokensPerMultiToken);

        int[] smallIntegerSubTokenBitmasks = compiledSchema.smallIntegerSubTokenBitmasks;
        int bitmask = smallIntegerSubTokenBitmasks[22];
        assertNotEquals("DD bitmask should be set for 22", 0x00, bitmask & DD_bitmask);
        assertEquals("MM bitmask should not be set for 22", 0x00, bitmask & MM_bitmask);
        assertEquals("YYYY bitmask should not be set for 22", 0x00, bitmask & YYYY_bitmask);
        assertEquals("hh bitmask should not be set for 22", 0x00, bitmask & hh_bitmask);
        assertNotEquals("HH bitmask should be set for 22", 0x00, bitmask & HH_bitmask);
        assertNotEquals("mm bitmask should be set for 22", 0x00, bitmask & mm_bitmask);
        assertNotEquals("ss bitmask should be set for 22", 0x00, bitmask & ss_bitmask);
        assertNotEquals("int_bitmask should be set for 22", 0x00, bitmask & int_bitmask);

        bitmask = smallIntegerSubTokenBitmasks[50];
        assertEquals("DD bitmask should not be set for 50", 0x00, bitmask & DD_bitmask);
        assertEquals("DD bitmask should not be set for 50", 0x00, bitmask & MM_bitmask);
        assertEquals("YYYY bitmask should not be set for 50", 0x00, bitmask & YYYY_bitmask);
        assertEquals("hh bitmask should not be set for 50", 0x00, bitmask & hh_bitmask);
        assertNotEquals("mm bitmask should be set for 50", 0x00, bitmask & mm_bitmask);
        assertNotEquals("ss bitmask should be set for 50", 0x00, bitmask & ss_bitmask);
        assertNotEquals("int_bitmask should be set for 50", 0x00, bitmask & int_bitmask);

        int bitmaskForInteger = getBitmaskForInteger(2000, compiledSchema);
        assertNotEquals(0x00, bitmaskForInteger & YYYY_bitmask);
        assertEquals(0x00, bitmaskForInteger & TZOhhmm_bitmask);
        assertNotEquals("int_bitmask should be set for 2000", 0x00, bitmaskForInteger & int_bitmask);

        SubTokenDelimiterCharParsingInfo[] subTokenDelimiterCharParsingInfos = compiledSchema.subTokenDelimiterCharParsingInfos;
        assertNotNull(subTokenDelimiterCharParsingInfos);
        SubTokenDelimiterCharParsingInfo spaceDelimiterInfo = subTokenDelimiterCharParsingInfos[' '];

        SubTokenEvaluator<SubstringView> subTokenEvaluator_Space_0 = spaceDelimiterInfo.subTokenEvaluatorPerSubTokenIndices[0];
        assertNotEquals(0x00, Mon_subToken_bitmask & subTokenEvaluator_Space_0.bitmask);

        BitmaskRegistry<TokenType> tokenBitmaskRegistry = compiledSchema.tokenBitmaskRegistry;

        tokenBitmaskRegistry.getAllRegisteredTypes().forEach(tokenType -> {
            try {
                tokenType.getHigherLevelBitmaskByPosition(compiledSchema.maxTokensPerMultiToken - 1);
            } catch (Exception e) {
                fail("Should not have thrown exception: " + e.getMessage());
            }
        });

        int ipv4_bitmask = tokenBitmaskRegistry.getBitmask("IPv4");
        int uuid_bitmask = tokenBitmaskRegistry.getBitmask("UUID_standard");
        int Mon_token_bitmask = tokenBitmaskRegistry.getBitmask("Mon");

        SubTokenDelimiterCharParsingInfo dotDelimiterInfo = subTokenDelimiterCharParsingInfos['.'];
        int[] tokenBitmaskPerSubTokenIndex_dot = dotDelimiterInfo.tokenBitmaskPerSubTokenIndex;
        SubTokenDelimiterCharParsingInfo dashDelimiterInfo = subTokenDelimiterCharParsingInfos['-'];
        int[] tokenBitmaskPerSubTokenIndex_dash = dashDelimiterInfo.tokenBitmaskPerSubTokenIndex;
        int[] tokenBitmaskPerSubTokenIndex_space = spaceDelimiterInfo.tokenBitmaskPerSubTokenIndex;

        // IPv4
        assertNotEquals(0x00, tokenBitmaskPerSubTokenIndex_dot[0] & ipv4_bitmask);
        assertEquals(0x00, tokenBitmaskPerSubTokenIndex_space[0] & ipv4_bitmask);
        assertNotEquals(0x00, tokenBitmaskPerSubTokenIndex_dot[1] & ipv4_bitmask);
        assertEquals(0x00, tokenBitmaskPerSubTokenIndex_space[1] & ipv4_bitmask);
        assertNotEquals(0x00, tokenBitmaskPerSubTokenIndex_dot[2] & ipv4_bitmask);
        assertEquals(0x00, tokenBitmaskPerSubTokenIndex_space[2] & ipv4_bitmask);
        // the fourth sub-token in IPv4 should be tested against the token (not sub-token) delimiter info because it is the last sub-token
        assertEquals(0x00, tokenBitmaskPerSubTokenIndex_dot[3] & ipv4_bitmask);
        assertNotEquals(0x00, tokenBitmaskPerSubTokenIndex_space[3] & ipv4_bitmask);

        // UUID
        assertEquals(0x00, tokenBitmaskPerSubTokenIndex_dot[0] & uuid_bitmask);
        assertNotEquals(0x00, tokenBitmaskPerSubTokenIndex_dash[0] & uuid_bitmask);
        assertEquals(0x00, tokenBitmaskPerSubTokenIndex_dot[1] & uuid_bitmask);
        assertNotEquals(0x00, tokenBitmaskPerSubTokenIndex_dash[1] & uuid_bitmask);
        assertEquals(0x00, tokenBitmaskPerSubTokenIndex_dot[2] & uuid_bitmask);
        assertNotEquals(0x00, tokenBitmaskPerSubTokenIndex_dash[2] & uuid_bitmask);

        // UUID standard format is defined in the schema as: "(%X{8})-(%X{4})-(%X{4})-(%X{4})-(%X{12})"
        int tokenBitmaskForUUID = tokenBitmaskRegistry.getCombinedBitmask();
        String testUuid = "123e4567-e89b-12d3-a456-426614174000";
        SubTokenEvaluator<SubstringView>[] dashTokenSubTokenEvaluatorPerIndex = dashDelimiterInfo.subTokenEvaluatorPerSubTokenIndices;
        assertNotNull(dashTokenSubTokenEvaluatorPerIndex);
        SubTokenEvaluator<SubstringView> subTokenEvaluator = dashTokenSubTokenEvaluatorPerIndex[0];
        assertNotNull(subTokenEvaluator);
        int firstSubTokenBitmask = subTokenBitmaskRegistry.getBitmask(
            org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.SubTokenType.ADHOC_PREFIX + "%X{8}"
        );
        assertNotEquals(0x00, firstSubTokenBitmask);
        assertTrue(subTokenEvaluator.evaluate(new SubstringView(testUuid, 0, 8)) >= 0);
        assertTrue(subTokenEvaluator.evaluate(new SubstringView(testUuid, 0, 7)) < 0);
        assertNotEquals(0x00, subTokenEvaluator.bitmask & firstSubTokenBitmask);
        tokenBitmaskForUUID &= subTokenBitmaskRegistry.getHigherLevelBitmaskByPosition(firstSubTokenBitmask, 0);
        subTokenEvaluator = dashTokenSubTokenEvaluatorPerIndex[1];
        assertNotNull(subTokenEvaluator);
        int middleSubTokensBitmask = subTokenBitmaskRegistry.getBitmask(
            org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.SubTokenType.ADHOC_PREFIX + "%X{4}"
        );
        assertTrue(subTokenEvaluator.evaluate(new SubstringView(testUuid, 9, 13)) >= 0);
        assertTrue(subTokenEvaluator.evaluate(new SubstringView(testUuid, 9, 12)) < 0);
        assertNotEquals(0x00, subTokenEvaluator.bitmask & middleSubTokensBitmask);
        tokenBitmaskForUUID &= subTokenBitmaskRegistry.getHigherLevelBitmaskByPosition(middleSubTokensBitmask, 1);
        subTokenEvaluator = dashTokenSubTokenEvaluatorPerIndex[2];
        assertNotNull(subTokenEvaluator);
        assertTrue(subTokenEvaluator.evaluate(new SubstringView(testUuid, 14, 18)) >= 0);
        assertTrue(subTokenEvaluator.evaluate(new SubstringView(testUuid, 14, 17)) < 0);
        assertNotEquals(0x00, subTokenEvaluator.bitmask & middleSubTokensBitmask);
        tokenBitmaskForUUID &= subTokenBitmaskRegistry.getHigherLevelBitmaskByPosition(middleSubTokensBitmask, 2);
        subTokenEvaluator = dashTokenSubTokenEvaluatorPerIndex[3];
        assertNotNull(subTokenEvaluator);
        assertTrue(subTokenEvaluator.evaluate(new SubstringView(testUuid, 19, 23)) >= 0);
        assertTrue(subTokenEvaluator.evaluate(new SubstringView(testUuid, 19, 22)) < 0);
        assertNotEquals(0x00, subTokenEvaluator.bitmask & middleSubTokensBitmask);
        tokenBitmaskForUUID &= subTokenBitmaskRegistry.getHigherLevelBitmaskByPosition(middleSubTokensBitmask, 3);
        // the fifth sub-token of a UUID should be tested against the token (not sub-token) delimiter info because it is the last sub-token
        int lastSubTokenBitmask = subTokenBitmaskRegistry.getBitmask(
            org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.SubTokenType.ADHOC_PREFIX + "%X{12}"
        );
        subTokenEvaluator = dashTokenSubTokenEvaluatorPerIndex[4];
        assertNull(subTokenEvaluator);
        subTokenEvaluator = spaceDelimiterInfo.subTokenEvaluatorPerSubTokenIndices[4];
        assertNotNull(subTokenEvaluator);
        assertTrue(subTokenEvaluator.evaluate(new SubstringView(testUuid, 24, 36)) >= 0);
        assertTrue(subTokenEvaluator.evaluate(new SubstringView(testUuid, 24, 35)) < 0);
        assertNotEquals(0x00, subTokenEvaluator.bitmask & lastSubTokenBitmask);
        tokenBitmaskForUUID &= subTokenBitmaskRegistry.getHigherLevelBitmaskByPosition(lastSubTokenBitmask, 4);
        tokenBitmaskForUUID &= compiledSchema.subTokenCountToTokenBitmask[4];
        assertEquals("The combined UUID token bitmask should match the expected UUID bitmask", uuid_bitmask, tokenBitmaskForUUID);

        // Mon
        assertNotEquals(0x00, tokenBitmaskPerSubTokenIndex_space[0] & Mon_token_bitmask);
        assertEquals(0x00, tokenBitmaskPerSubTokenIndex_dot[0] & Mon_token_bitmask);
        assertEquals(0x00, tokenBitmaskPerSubTokenIndex_dash[0] & Mon_token_bitmask);
        assertEquals(0x00, tokenBitmaskPerSubTokenIndex_space[1] & Mon_token_bitmask);
        assertEquals(0x00, tokenBitmaskPerSubTokenIndex_dot[1] & Mon_token_bitmask);
        assertEquals(0x00, tokenBitmaskPerSubTokenIndex_dash[1] & Mon_token_bitmask);
        // todo - verify that using the single Mon sub-token bitmask we get the expected Mon token bitmask

        int[] subTokenCountToTokenBitmask = compiledSchema.subTokenCountToTokenBitmask;
        assertEquals(0x00, subTokenCountToTokenBitmask[2] & ipv4_bitmask);
        assertNotEquals(0x00, subTokenCountToTokenBitmask[3] & ipv4_bitmask);
        assertEquals(0x00, subTokenCountToTokenBitmask[4] & ipv4_bitmask);
        assertEquals(0x00, subTokenCountToTokenBitmask[3] & uuid_bitmask);
        assertNotEquals(0x00, subTokenCountToTokenBitmask[4] & uuid_bitmask);
        assertEquals(0x00, subTokenCountToTokenBitmask[5] & uuid_bitmask);

        assertEquals(7, compiledSchema.maxSubTokensPerMultiToken);
        BitmaskRegistry<MultiTokenType> multiTokenBitmaskRegistry = compiledSchema.multiTokenBitmaskRegistry;
        int timestamp1_bitmask = multiTokenBitmaskRegistry.getBitmask("timestamp1");
        MultiTokenType timestamp1Type = multiTokenBitmaskRegistry.getHighestPriorityType(timestamp1_bitmask);
        assertEquals(7, timestamp1Type.getNumSubTokens());
        TimestampFormat timestamp1Format = timestamp1Type.getTimestampFormat();
        assertEquals(7, timestamp1Format.getNumTimestampComponents());
        int[] timestampComponentsOrder = timestamp1Format.getTimestampComponentsOrder();
        assertEquals(TimestampComponentType.values().length, timestampComponentsOrder.length);
        // "timestamp1" type format is: "$Mon, $DD $YYYY $timeS $AP"
        assertEquals("MMM, dd yyyy hh:mm:ss a", timestamp1Format.getJavaTimeFormat());
        assertEquals(0, timestampComponentsOrder[TimestampComponentType.MONTH_CODE]);
        assertEquals(1, timestampComponentsOrder[TimestampComponentType.DAY_CODE]);
        assertEquals(2, timestampComponentsOrder[TimestampComponentType.YEAR_CODE]);
        assertEquals(3, timestampComponentsOrder[TimestampComponentType.HOUR_CODE]);
        assertEquals(4, timestampComponentsOrder[TimestampComponentType.MINUTE_CODE]);
        assertEquals(5, timestampComponentsOrder[TimestampComponentType.SECOND_CODE]);
        assertEquals(6, timestampComponentsOrder[TimestampComponentType.AM_PM_CODE]);
    }

    public void testCreateTimestampFormat_withBracketLiterals() {
        Schema schema = Schema.getInstance();
        String rawFormat = "[$date2  {$timeMS} $TZOhhmm]";
        List<Object> formatParts = PatternUtils.parseMultiTokenFormat(rawFormat, schema.getTokenTypes(), schema.getTokenBoundaryChars());
        MultiTokenFormat multiTokenFormat = new MultiTokenFormat(rawFormat, formatParts);

        TimestampFormat result = SchemaCompiler.createTimestampFormat(multiTokenFormat);

        // bracket literals should be escaped in the Java time format; double-space should be preserved
        assertEquals("'['yyyy-MM-dd  '{'hh:mm:ss.SSS'}' Z']'", result.getJavaTimeFormat());
        int[] order = result.getTimestampComponentsOrder();
        assertEquals(0, order[TimestampComponentType.YEAR_CODE]);
        assertEquals(1, order[TimestampComponentType.MONTH_CODE]);
        assertEquals(2, order[TimestampComponentType.DAY_CODE]);
        assertEquals(3, order[TimestampComponentType.HOUR_CODE]);
        assertEquals(4, order[TimestampComponentType.MINUTE_CODE]);
        assertEquals(5, order[TimestampComponentType.SECOND_CODE]);
        assertEquals(6, order[TimestampComponentType.MILLISECOND_CODE]);
    }

    private static int getBitmaskForInteger(int value, CompiledSchema compiledSchema) {
        int[] integerSubTokenBitmaskArrayRanges = compiledSchema.integerSubTokenBitmaskArrayRanges;
        int[] integerSubTokenBitmasks = compiledSchema.integerSubTokenBitmasks;
        for (int i = 0; i < integerSubTokenBitmaskArrayRanges.length; i++) {
            if (value <= integerSubTokenBitmaskArrayRanges[i]) {
                if (i == integerSubTokenBitmaskArrayRanges.length - 1) {
                    throw new IllegalArgumentException("Value " + value + " exceeds maximum range defined in schema.");
                }
                return integerSubTokenBitmasks[i];
            }
        }
        throw new IllegalArgumentException("Value " + value + " is below minimum range defined in schema.");
    }
}
