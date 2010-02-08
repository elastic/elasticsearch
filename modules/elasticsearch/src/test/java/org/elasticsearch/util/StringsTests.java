/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.util;

/**
 * Tests for {@link Strings}.
 *
 * @author kimchy (Shay Banon)
 */
public class StringsTests {

//    @Test public void testHasTextBlank() throws Exception {
//        String blank = "          ";
//        assertEquals(false, Strings.hasText(blank));
//    }
//
//    @Test public void testHasTextNullEmpty() throws Exception {
//        assertEquals(false, Strings.hasText(null));
//        assertEquals(false, Strings.hasText(""));
//    }
//
//    @Test public void testHasTextValid() throws Exception {
//        assertEquals(true, Strings.hasText("t"));
//    }
//
//    @Test public void testContainsWhitespace() throws Exception {
//        assertFalse(Strings.containsWhitespace(null));
//        assertFalse(Strings.containsWhitespace(""));
//        assertFalse(Strings.containsWhitespace("a"));
//        assertFalse(Strings.containsWhitespace("abc"));
//        assertTrue(Strings.containsWhitespace(" "));
//        assertTrue(Strings.containsWhitespace(" a"));
//        assertTrue(Strings.containsWhitespace("abc "));
//        assertTrue(Strings.containsWhitespace("a b"));
//        assertTrue(Strings.containsWhitespace("a  b"));
//    }
//
//    @Test public void testTrimWhitespace() throws Exception {
//        assertEquals(null, Strings.trimWhitespace(null));
//        assertEquals("", Strings.trimWhitespace(""));
//        assertEquals("", Strings.trimWhitespace(" "));
//        assertEquals("", Strings.trimWhitespace("\t"));
//        assertEquals("a", Strings.trimWhitespace(" a"));
//        assertEquals("a", Strings.trimWhitespace("a "));
//        assertEquals("a", Strings.trimWhitespace(" a "));
//        assertEquals("a b", Strings.trimWhitespace(" a b "));
//        assertEquals("a b  c", Strings.trimWhitespace(" a b  c "));
//    }
//
//    @Test public void testTrimAllWhitespace() throws Exception {
//        assertEquals("", Strings.trimAllWhitespace(""));
//        assertEquals("", Strings.trimAllWhitespace(" "));
//        assertEquals("", Strings.trimAllWhitespace("\t"));
//        assertEquals("a", Strings.trimAllWhitespace(" a"));
//        assertEquals("a", Strings.trimAllWhitespace("a "));
//        assertEquals("a", Strings.trimAllWhitespace(" a "));
//        assertEquals("ab", Strings.trimAllWhitespace(" a b "));
//        assertEquals("abc", Strings.trimAllWhitespace(" a b  c "));
//    }
//
//    @Test public void testTrimLeadingWhitespace() throws Exception {
//        assertEquals(null, Strings.trimLeadingWhitespace(null));
//        assertEquals("", Strings.trimLeadingWhitespace(""));
//        assertEquals("", Strings.trimLeadingWhitespace(" "));
//        assertEquals("", Strings.trimLeadingWhitespace("\t"));
//        assertEquals("a", Strings.trimLeadingWhitespace(" a"));
//        assertEquals("a ", Strings.trimLeadingWhitespace("a "));
//        assertEquals("a ", Strings.trimLeadingWhitespace(" a "));
//        assertEquals("a b ", Strings.trimLeadingWhitespace(" a b "));
//        assertEquals("a b  c ", Strings.trimLeadingWhitespace(" a b  c "));
//    }
//
//    @Test public void testTrimTrailingWhitespace() throws Exception {
//        assertEquals(null, Strings.trimTrailingWhitespace(null));
//        assertEquals("", Strings.trimTrailingWhitespace(""));
//        assertEquals("", Strings.trimTrailingWhitespace(" "));
//        assertEquals("", Strings.trimTrailingWhitespace("\t"));
//        assertEquals("a", Strings.trimTrailingWhitespace("a "));
//        assertEquals(" a", Strings.trimTrailingWhitespace(" a"));
//        assertEquals(" a", Strings.trimTrailingWhitespace(" a "));
//        assertEquals(" a b", Strings.trimTrailingWhitespace(" a b "));
//        assertEquals(" a b  c", Strings.trimTrailingWhitespace(" a b  c "));
//    }
//
//    @Test public void testTrimLeadingCharacter() throws Exception {
//        assertEquals(null, Strings.trimLeadingCharacter(null, ' '));
//        assertEquals("", Strings.trimLeadingCharacter("", ' '));
//        assertEquals("", Strings.trimLeadingCharacter(" ", ' '));
//        assertEquals("\t", Strings.trimLeadingCharacter("\t", ' '));
//        assertEquals("a", Strings.trimLeadingCharacter(" a", ' '));
//        assertEquals("a ", Strings.trimLeadingCharacter("a ", ' '));
//        assertEquals("a ", Strings.trimLeadingCharacter(" a ", ' '));
//        assertEquals("a b ", Strings.trimLeadingCharacter(" a b ", ' '));
//        assertEquals("a b  c ", Strings.trimLeadingCharacter(" a b  c ", ' '));
//    }
//
//    @Test public void testTrimTrailingCharacter() throws Exception {
//        assertEquals(null, Strings.trimTrailingCharacter(null, ' '));
//        assertEquals("", Strings.trimTrailingCharacter("", ' '));
//        assertEquals("", Strings.trimTrailingCharacter(" ", ' '));
//        assertEquals("\t", Strings.trimTrailingCharacter("\t", ' '));
//        assertEquals("a", Strings.trimTrailingCharacter("a ", ' '));
//        assertEquals(" a", Strings.trimTrailingCharacter(" a", ' '));
//        assertEquals(" a", Strings.trimTrailingCharacter(" a ", ' '));
//        assertEquals(" a b", Strings.trimTrailingCharacter(" a b ", ' '));
//        assertEquals(" a b  c", Strings.trimTrailingCharacter(" a b  c ", ' '));
//    }
//
//    @Test public void testCountOccurrencesOf() {
//        assertTrue("nullx2 = 0",
//                Strings.countOccurrencesOf(null, null) == 0);
//        assertTrue("null string = 0",
//                Strings.countOccurrencesOf("s", null) == 0);
//        assertTrue("null substring = 0",
//                Strings.countOccurrencesOf(null, "s") == 0);
//        String s = "erowoiueoiur";
//        assertTrue("not found = 0",
//                Strings.countOccurrencesOf(s, "WERWER") == 0);
//        assertTrue("not found char = 0",
//                Strings.countOccurrencesOf(s, "x") == 0);
//        assertTrue("not found ws = 0",
//                Strings.countOccurrencesOf(s, " ") == 0);
//        assertTrue("not found empty string = 0",
//                Strings.countOccurrencesOf(s, "") == 0);
//        assertTrue("found char=2", Strings.countOccurrencesOf(s, "e") == 2);
//        assertTrue("found substring=2",
//                Strings.countOccurrencesOf(s, "oi") == 2);
//        assertTrue("found substring=2",
//                Strings.countOccurrencesOf(s, "oiu") == 2);
//        assertTrue("found substring=3",
//                Strings.countOccurrencesOf(s, "oiur") == 1);
//        assertTrue("test last", Strings.countOccurrencesOf(s, "r") == 2);
//    }
//
//    @Test public void testReplace() throws Exception {
//        String inString = "a6AazAaa77abaa";
//        String oldPattern = "aa";
//        String newPattern = "foo";
//
//        // Simple replace
//        String s = Strings.replace(inString, oldPattern, newPattern);
//        assertTrue("Replace 1 worked", s.equals("a6AazAfoo77abfoo"));
//
//        // Non match: no change
//        s = Strings.replace(inString, "qwoeiruqopwieurpoqwieur", newPattern);
//        assertTrue("Replace non matched is equal", s.equals(inString));
//
//        // Null new pattern: should ignore
//        s = Strings.replace(inString, oldPattern, null);
//        assertTrue("Replace non matched is equal", s.equals(inString));
//
//        // Null old pattern: should ignore
//        s = Strings.replace(inString, null, newPattern);
//        assertTrue("Replace non matched is equal", s.equals(inString));
//    }
//
//    @Test public void testDelete() throws Exception {
//        String inString = "The quick brown fox jumped over the lazy dog";
//
//        String noThe = Strings.delete(inString, "the");
//        assertTrue("Result has no the [" + noThe + "]",
//                noThe.equals("The quick brown fox jumped over  lazy dog"));
//
//        String nohe = Strings.delete(inString, "he");
//        assertTrue("Result has no he [" + nohe + "]",
//                nohe.equals("T quick brown fox jumped over t lazy dog"));
//
//        String nosp = Strings.delete(inString, " ");
//        assertTrue("Result has no spaces",
//                nosp.equals("Thequickbrownfoxjumpedoverthelazydog"));
//
//        String killEnd = Strings.delete(inString, "dog");
//        assertTrue("Result has no dog",
//                killEnd.equals("The quick brown fox jumped over the lazy "));
//
//        String mismatch = Strings.delete(inString, "dxxcxcxog");
//        assertTrue("Result is unchanged", mismatch.equals(inString));
//
//        String nochange = Strings.delete(inString, "");
//        assertTrue("Result is unchanged", nochange.equals(inString));
//    }
//
//    @Test public void testDeleteAny() throws Exception {
//        String inString = "Able was I ere I saw Elba";
//
//        String res = Strings.deleteAny(inString, "I");
//        assertTrue("Result has no Is [" + res + "]", res.equals("Able was  ere  saw Elba"));
//
//        res = Strings.deleteAny(inString, "AeEba!");
//        assertTrue("Result has no Is [" + res + "]", res.equals("l ws I r I sw l"));
//
//        String mismatch = Strings.deleteAny(inString, "#@$#$^");
//        assertTrue("Result is unchanged", mismatch.equals(inString));
//
//        String whitespace = "This is\n\n\n    \t   a messagy string with whitespace\n";
//        assertTrue("Has CR", whitespace.indexOf("\n") != -1);
//        assertTrue("Has tab", whitespace.indexOf("\t") != -1);
//        assertTrue("Has  sp", whitespace.indexOf(" ") != -1);
//        String cleaned = Strings.deleteAny(whitespace, "\n\t ");
//        assertTrue("Has no CR", cleaned.indexOf("\n") == -1);
//        assertTrue("Has no tab", cleaned.indexOf("\t") == -1);
//        assertTrue("Has no sp", cleaned.indexOf(" ") == -1);
//        assertTrue("Still has chars", cleaned.length() > 10);
//    }
//
//
//    @Test public void testQuote() {
//        assertEquals("'myString'", Strings.quote("myString"));
//        assertEquals("''", Strings.quote(""));
//        assertNull(Strings.quote(null));
//    }
//
//    @Test public void testQuoteIfString() {
//        assertEquals("'myString'", Strings.quoteIfString("myString"));
//        assertEquals("''", Strings.quoteIfString(""));
//        assertEquals(5, Strings.quoteIfString(5));
//        assertNull(Strings.quoteIfString(null));
//    }
//
//    @Test public void testUnqualify() {
//        String qualified = "i.am.not.unqualified";
//        assertEquals("unqualified", Strings.unqualify(qualified));
//    }
//
//    @Test public void testCapitalize() {
//        String capitalized = "i am not capitalized";
//        assertEquals("I am not capitalized", Strings.capitalize(capitalized));
//    }
//
//    @Test public void testUncapitalize() {
//        String capitalized = "I am capitalized";
//        assertEquals("i am capitalized", Strings.uncapitalize(capitalized));
//    }
//
//    @Test public void testGetFilename() {
//        assertEquals(null, Strings.getFilename(null));
//        assertEquals("", Strings.getFilename(""));
//        assertEquals("myfile", Strings.getFilename("myfile"));
//        assertEquals("myfile", Strings.getFilename("mypath/myfile"));
//        assertEquals("myfile.", Strings.getFilename("myfile."));
//        assertEquals("myfile.", Strings.getFilename("mypath/myfile."));
//        assertEquals("myfile.txt", Strings.getFilename("myfile.txt"));
//        assertEquals("myfile.txt", Strings.getFilename("mypath/myfile.txt"));
//    }
//
//    @Test public void testGetFilenameExtension() {
//        assertEquals(null, Strings.getFilenameExtension(null));
//        assertEquals(null, Strings.getFilenameExtension(""));
//        assertEquals(null, Strings.getFilenameExtension("myfile"));
//        assertEquals(null, Strings.getFilenameExtension("myPath/myfile"));
//        assertEquals("", Strings.getFilenameExtension("myfile."));
//        assertEquals("", Strings.getFilenameExtension("myPath/myfile."));
//        assertEquals("txt", Strings.getFilenameExtension("myfile.txt"));
//        assertEquals("txt", Strings.getFilenameExtension("mypath/myfile.txt"));
//    }
//
//    @Test public void testStripFilenameExtension() {
//        assertEquals(null, Strings.stripFilenameExtension(null));
//        assertEquals("", Strings.stripFilenameExtension(""));
//        assertEquals("myfile", Strings.stripFilenameExtension("myfile"));
//        assertEquals("mypath/myfile", Strings.stripFilenameExtension("mypath/myfile"));
//        assertEquals("myfile", Strings.stripFilenameExtension("myfile."));
//        assertEquals("mypath/myfile", Strings.stripFilenameExtension("mypath/myfile."));
//        assertEquals("myfile", Strings.stripFilenameExtension("myfile.txt"));
//        assertEquals("mypath/myfile", Strings.stripFilenameExtension("mypath/myfile.txt"));
//    }
//
//    @Test public void testCleanPath() {
//        assertEquals("mypath/myfile", Strings.cleanPath("mypath/myfile"));
//        assertEquals("mypath/myfile", Strings.cleanPath("mypath\\myfile"));
//        assertEquals("mypath/myfile", Strings.cleanPath("mypath/../mypath/myfile"));
//        assertEquals("mypath/myfile", Strings.cleanPath("mypath/myfile/../../mypath/myfile"));
//        assertEquals("../mypath/myfile", Strings.cleanPath("../mypath/myfile"));
//        assertEquals("../mypath/myfile", Strings.cleanPath("../mypath/../mypath/myfile"));
//        assertEquals("../mypath/myfile", Strings.cleanPath("mypath/../../mypath/myfile"));
//        assertEquals("/../mypath/myfile", Strings.cleanPath("/../mypath/myfile"));
//    }
//
//    @Test public void testPathEquals() {
//        assertTrue("Must be true for the same strings",
//                Strings.pathEquals("/dummy1/dummy2/dummy3",
//                        "/dummy1/dummy2/dummy3"));
//        assertTrue("Must be true for the same win strings",
//                Strings.pathEquals("C:\\dummy1\\dummy2\\dummy3",
//                        "C:\\dummy1\\dummy2\\dummy3"));
//        assertTrue("Must be true for one top path on 1",
//                Strings.pathEquals("/dummy1/bin/../dummy2/dummy3",
//                        "/dummy1/dummy2/dummy3"));
//        assertTrue("Must be true for one win top path on 2",
//                Strings.pathEquals("C:\\dummy1\\dummy2\\dummy3",
//                        "C:\\dummy1\\bin\\..\\dummy2\\dummy3"));
//        assertTrue("Must be true for two top paths on 1",
//                Strings.pathEquals("/dummy1/bin/../dummy2/bin/../dummy3",
//                        "/dummy1/dummy2/dummy3"));
//        assertTrue("Must be true for two win top paths on 2",
//                Strings.pathEquals("C:\\dummy1\\dummy2\\dummy3",
//                        "C:\\dummy1\\bin\\..\\dummy2\\bin\\..\\dummy3"));
//        assertTrue("Must be true for double top paths on 1",
//                Strings.pathEquals("/dummy1/bin/tmp/../../dummy2/dummy3",
//                        "/dummy1/dummy2/dummy3"));
//        assertTrue("Must be true for double top paths on 2 with similarity",
//                Strings.pathEquals("/dummy1/dummy2/dummy3",
//                        "/dummy1/dum/dum/../../dummy2/dummy3"));
//        assertTrue("Must be true for current paths",
//                Strings.pathEquals("./dummy1/dummy2/dummy3",
//                        "dummy1/dum/./dum/../../dummy2/dummy3"));
//        assertFalse("Must be false for relative/absolute paths",
//                Strings.pathEquals("./dummy1/dummy2/dummy3",
//                        "/dummy1/dum/./dum/../../dummy2/dummy3"));
//        assertFalse("Must be false for different strings",
//                Strings.pathEquals("/dummy1/dummy2/dummy3",
//                        "/dummy1/dummy4/dummy3"));
//        assertFalse("Must be false for one false path on 1",
//                Strings.pathEquals("/dummy1/bin/tmp/../dummy2/dummy3",
//                        "/dummy1/dummy2/dummy3"));
//        assertFalse("Must be false for one false win top path on 2",
//                Strings.pathEquals("C:\\dummy1\\dummy2\\dummy3",
//                        "C:\\dummy1\\bin\\tmp\\..\\dummy2\\dummy3"));
//        assertFalse("Must be false for top path on 1 + difference",
//                Strings.pathEquals("/dummy1/bin/../dummy2/dummy3",
//                        "/dummy1/dummy2/dummy4"));
//    }
//
//    @Test public void testConcatenateStringArrays() {
//        String[] input1 = new String[]{"myString2"};
//        String[] input2 = new String[]{"myString1", "myString2"};
//        String[] result = Strings.concatenateStringArrays(input1, input2);
//        assertEquals(3, result.length);
//        assertEquals("myString2", result[0]);
//        assertEquals("myString1", result[1]);
//        assertEquals("myString2", result[2]);
//
//        assertArrayEquals(input1, Strings.concatenateStringArrays(input1, null));
//        assertArrayEquals(input2, Strings.concatenateStringArrays(null, input2));
//        assertNull(Strings.concatenateStringArrays(null, null));
//    }
//
//    @Test public void testMergeStringArrays() {
//        String[] input1 = new String[]{"myString2"};
//        String[] input2 = new String[]{"myString1", "myString2"};
//        String[] result = Strings.mergeStringArrays(input1, input2);
//        assertEquals(2, result.length);
//        assertEquals("myString2", result[0]);
//        assertEquals("myString1", result[1]);
//
//        assertArrayEquals(input1, Strings.mergeStringArrays(input1, null));
//        assertArrayEquals(input2, Strings.mergeStringArrays(null, input2));
//        assertNull(Strings.mergeStringArrays(null, null));
//    }
//
//    @Test public void testSortStringArray() {
//        String[] input = new String[]{"myString2"};
//        input = Strings.addStringToArray(input, "myString1");
//        assertEquals("myString2", input[0]);
//        assertEquals("myString1", input[1]);
//
//        Strings.sortStringArray(input);
//        assertEquals("myString1", input[0]);
//        assertEquals("myString2", input[1]);
//    }
//
//    @Test public void testRemoveDuplicateStrings() {
//        String[] input = new String[]{"myString2", "myString1", "myString2"};
//        input = Strings.removeDuplicateStrings(input);
//        assertEquals("myString1", input[0]);
//        assertEquals("myString2", input[1]);
//    }
//
//    @Test public void testSplitArrayElementsIntoProperties() {
//        String[] input = new String[]{"key1=value1 ", "key2 =\"value2\""};
//        Properties result = Strings.splitArrayElementsIntoProperties(input, "=");
//        assertEquals("value1", result.getProperty("key1"));
//        assertEquals("\"value2\"", result.getProperty("key2"));
//    }
//
//    @Test public void testSplitArrayElementsIntoPropertiesAndDeletedChars() {
//        String[] input = new String[]{"key1=value1 ", "key2 =\"value2\""};
//        Properties result = Strings.splitArrayElementsIntoProperties(input, "=", "\"");
//        assertEquals("value1", result.getProperty("key1"));
//        assertEquals("value2", result.getProperty("key2"));
//    }
//
//    @Test public void testTokenizeToStringArray() {
//        String[] sa = Strings.tokenizeToStringArray("a,b , ,c", ",");
//        assertEquals(3, sa.length);
//        assertTrue("components are correct",
//                sa[0].equals("a") && sa[1].equals("b") && sa[2].equals("c"));
//    }
//
//    @Test public void testTokenizeToStringArrayWithNotIgnoreEmptyTokens() {
//        String[] sa = Strings.tokenizeToStringArray("a,b , ,c", ",", true, false);
//        assertEquals(4, sa.length);
//        assertTrue("components are correct",
//                sa[0].equals("a") && sa[1].equals("b") && sa[2].equals("") && sa[3].equals("c"));
//    }
//
//    @Test public void testTokenizeToStringArrayWithNotTrimTokens() {
//        String[] sa = Strings.tokenizeToStringArray("a,b ,c", ",", false, true);
//        assertEquals(3, sa.length);
//        assertTrue("components are correct",
//                sa[0].equals("a") && sa[1].equals("b ") && sa[2].equals("c"));
//    }
//
//    @Test public void testCommaDelimitedListToStringArrayWithNullProducesEmptyArray() {
//        String[] sa = Strings.commaDelimitedListToStringArray(null);
//        assertTrue("String array isn't null with null input", sa != null);
//        assertTrue("String array length == 0 with null input", sa.length == 0);
//    }
//
//    @Test public void testCommaDelimitedListToStringArrayWithEmptyStringProducesEmptyArray() {
//        String[] sa = Strings.commaDelimitedListToStringArray("");
//        assertTrue("String array isn't null with null input", sa != null);
//        assertTrue("String array length == 0 with null input", sa.length == 0);
//    }
//
//    private void testStringArrayReverseTransformationMatches(String[] sa) {
//        String[] reverse =
//                Strings.commaDelimitedListToStringArray(Strings.arrayToCommaDelimitedString(sa));
//        assertEquals("Reverse transformation is equal",
//                Arrays.asList(sa),
//                Arrays.asList(reverse));
//    }
//
//    @Test public void testDelimitedListToStringArrayWithComma() {
//        String[] sa = Strings.delimitedListToStringArray("a,b", ",");
//        assertEquals(2, sa.length);
//        assertEquals("a", sa[0]);
//        assertEquals("b", sa[1]);
//    }
//
//    @Test public void testDelimitedListToStringArrayWithSemicolon() {
//        String[] sa = Strings.delimitedListToStringArray("a;b", ";");
//        assertEquals(2, sa.length);
//        assertEquals("a", sa[0]);
//        assertEquals("b", sa[1]);
//    }
//
//    @Test public void testDelimitedListToStringArrayWithEmptyString() {
//        String[] sa = Strings.delimitedListToStringArray("a,b", "");
//        assertEquals(3, sa.length);
//        assertEquals("a", sa[0]);
//        assertEquals(",", sa[1]);
//        assertEquals("b", sa[2]);
//    }
//
//    @Test public void testDelimitedListToStringArrayWithNullDelimiter() {
//        String[] sa = Strings.delimitedListToStringArray("a,b", null);
//        assertEquals(1, sa.length);
//        assertEquals("a,b", sa[0]);
//    }
//
//    @Test public void testCommaDelimitedListToStringArrayMatchWords() {
//        // Could read these from files
//        String[] sa = new String[]{"foo", "bar", "big"};
//        doTestCommaDelimitedListToStringArrayLegalMatch(sa);
//        testStringArrayReverseTransformationMatches(sa);
//
//        sa = new String[]{"a", "b", "c"};
//        doTestCommaDelimitedListToStringArrayLegalMatch(sa);
//        testStringArrayReverseTransformationMatches(sa);
//
//        // Test same words
//        sa = new String[]{"AA", "AA", "AA", "AA", "AA"};
//        doTestCommaDelimitedListToStringArrayLegalMatch(sa);
//        testStringArrayReverseTransformationMatches(sa);
//    }
//
//    @Test public void testCommaDelimitedListToStringArraySingleString() {
//        // Could read these from files
//        String s = "woeirqupoiewuropqiewuorpqiwueopriquwopeiurqopwieur";
//        String[] sa = Strings.commaDelimitedListToStringArray(s);
//        assertTrue("Found one String with no delimiters", sa.length == 1);
//        assertTrue("Single array entry matches input String with no delimiters",
//                sa[0].equals(s));
//    }
//
//    @Test public void testCommaDelimitedListToStringArrayWithOtherPunctuation() {
//        // Could read these from files
//        String[] sa = new String[]{"xcvwert4456346&*.", "///", ".!", ".", ";"};
//        doTestCommaDelimitedListToStringArrayLegalMatch(sa);
//    }
//
//    /**
//     * We expect to see the empty Strings in the output.
//     */
//    @Test public void testCommaDelimitedListToStringArrayEmptyStrings() {
//        // Could read these from files
//        String[] sa = Strings.commaDelimitedListToStringArray("a,,b");
//        assertEquals("a,,b produces array length 3", 3, sa.length);
//        assertTrue("components are correct",
//                sa[0].equals("a") && sa[1].equals("") && sa[2].equals("b"));
//
//        sa = new String[]{"", "", "a", ""};
//        doTestCommaDelimitedListToStringArrayLegalMatch(sa);
//    }
//
//    private void doTestCommaDelimitedListToStringArrayLegalMatch(String[] components) {
//        StringBuffer sbuf = new StringBuffer();
//        for (int i = 0; i < components.length; i++) {
//            if (i != 0) {
//                sbuf.append(",");
//            }
//            sbuf.append(components[i]);
//        }
//        String[] sa = Strings.commaDelimitedListToStringArray(sbuf.toString());
//        assertTrue("String array isn't null with legal match", sa != null);
//        assertEquals("String array length is correct with legal match", components.length, sa.length);
//        assertTrue("Output equals input", Arrays.equals(sa, components));
//    }
//
//    @Test public void testEndsWithIgnoreCase() {
//        String suffix = "fOo";
//        assertTrue(Strings.endsWithIgnoreCase("foo", suffix));
//        assertTrue(Strings.endsWithIgnoreCase("Foo", suffix));
//        assertTrue(Strings.endsWithIgnoreCase("barfoo", suffix));
//        assertTrue(Strings.endsWithIgnoreCase("barbarfoo", suffix));
//        assertTrue(Strings.endsWithIgnoreCase("barFoo", suffix));
//        assertTrue(Strings.endsWithIgnoreCase("barBarFoo", suffix));
//        assertTrue(Strings.endsWithIgnoreCase("barfoO", suffix));
//        assertTrue(Strings.endsWithIgnoreCase("barFOO", suffix));
//        assertTrue(Strings.endsWithIgnoreCase("barfOo", suffix));
//        assertFalse(Strings.endsWithIgnoreCase(null, suffix));
//        assertFalse(Strings.endsWithIgnoreCase("barfOo", null));
//        assertFalse(Strings.endsWithIgnoreCase("b", suffix));
//    }
//
//    @Test public void testParseLocaleStringSunnyDay() throws Exception {
//        Locale expectedLocale = Locale.UK;
//        Locale locale = Strings.parseLocaleString(expectedLocale.toString());
//        assertNotNull("When given a bona-fide Locale string, must not return null.", locale);
//        assertEquals(expectedLocale, locale);
//    }
//
//    @Test public void testParseLocaleStringWithMalformedLocaleString() throws Exception {
//        Locale locale = Strings.parseLocaleString("_banjo_on_my_knee");
//        assertNotNull("When given a malformed Locale string, must not return null.", locale);
//    }
//
//    @Test public void testParseLocaleStringWithEmptyLocaleStringYieldsNullLocale() throws Exception {
//        Locale locale = Strings.parseLocaleString("");
//        assertNull("When given an empty Locale string, must return null.", locale);
//    }
//
//    @Test public void testParseLocaleWithMultiValuedVariant() throws Exception {
//        final String variant = "proper_northern";
//        final String localeString = "en_GB_" + variant;
//        Locale locale = Strings.parseLocaleString(localeString);
//        assertEquals("Multi-valued variant portion of the Locale not extracted correctly.", variant, locale.getVariant());
//    }
//
//    @Test public void testParseLocaleWithMultiValuedVariantUsingSpacesAsSeparators() throws Exception {
//        final String variant = "proper northern";
//        final String localeString = "en GB " + variant;
//        Locale locale = Strings.parseLocaleString(localeString);
//        assertEquals("Multi-valued variant portion of the Locale not extracted correctly.", variant, locale.getVariant());
//    }
//
//    @Test public void testParseLocaleWithMultiValuedVariantUsingMixtureOfUnderscoresAndSpacesAsSeparators() throws Exception {
//        final String variant = "proper northern";
//        final String localeString = "en_GB_" + variant;
//        Locale locale = Strings.parseLocaleString(localeString);
//        assertEquals("Multi-valued variant portion of the Locale not extracted correctly.", variant, locale.getVariant());
//    }
//
//    @Test public void testParseLocaleWithMultiValuedVariantUsingSpacesAsSeparatorsWithLotsOfLeadingWhitespace() throws Exception {
//        final String variant = "proper northern";
//        final String localeString = "en GB            " + variant; // lots of whitespace
//        Locale locale = Strings.parseLocaleString(localeString);
//        assertEquals("Multi-valued variant portion of the Locale not extracted correctly.", variant, locale.getVariant());
//    }
//
//    @Test public void testParseLocaleWithMultiValuedVariantUsingUnderscoresAsSeparatorsWithLotsOfLeadingWhitespace() throws Exception {
//        final String variant = "proper_northern";
//        final String localeString = "en_GB_____" + variant; // lots of underscores
//        Locale locale = Strings.parseLocaleString(localeString);
//        assertEquals("Multi-valued variant portion of the Locale not extracted correctly.", variant, locale.getVariant());
//    }

}
