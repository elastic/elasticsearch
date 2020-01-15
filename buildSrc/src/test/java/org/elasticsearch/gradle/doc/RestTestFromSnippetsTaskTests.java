package org.elasticsearch.gradle.doc;

import org.elasticsearch.gradle.test.GradleUnitTestCase;
import org.gradle.api.InvalidUserDataException;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import static org.elasticsearch.gradle.doc.RestTestsFromSnippetsTask.replaceBlockQuote;

public class RestTestFromSnippetsTaskTests extends GradleUnitTestCase {
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    public void testInvalidBlockQuote() {
        String input = "\"foo\": \"\"\"bar\"";
        expectedEx.expect(InvalidUserDataException.class);
        expectedEx.expectMessage("Invalid block quote starting at 7 in:\n" + input);
        replaceBlockQuote(input);
    }

    public void testSimpleBlockQuote() {
        assertEquals("\"foo\": \"bort baz\"", replaceBlockQuote("\"foo\": \"\"\"bort baz\"\"\""));
    }

    public void testMultipleBlockQuotes() {
        assertEquals(
            "\"foo\": \"bort baz\", \"bar\": \"other\"",
            replaceBlockQuote("\"foo\": \"\"\"bort baz\"\"\", \"bar\": \"\"\"other\"\"\"")
        );
    }

    public void testEscapingInBlockQuote() {
        assertEquals("\"foo\": \"bort\\\" baz\"", replaceBlockQuote("\"foo\": \"\"\"bort\" baz\"\"\""));
        assertEquals("\"foo\": \"bort\\n baz\"", replaceBlockQuote("\"foo\": \"\"\"bort\n baz\"\"\""));
    }

    public void testIsDocWriteRequest() {
        assertTrue((boolean) RestTestsFromSnippetsTask.shouldAddShardFailureCheck("doc-index/_search"));
        assertFalse((boolean) RestTestsFromSnippetsTask.shouldAddShardFailureCheck("_cat"));
        assertFalse((boolean) RestTestsFromSnippetsTask.shouldAddShardFailureCheck("_ml/datafeeds/datafeed-id/_preview"));
    }
}
