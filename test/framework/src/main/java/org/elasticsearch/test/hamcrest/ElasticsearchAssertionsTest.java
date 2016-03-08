package org.elasticsearch.test.hamcrest;

import junit.framework.AssertionFailedError;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.core.Is.is;

public class ElasticsearchAssertionsTest extends ESTestCase {

    public void testThatAssertThrowsCorrectlyFailsOnNotHappenedException() {
        try {
            assertThrows(ElasticsearchException.class, () -> logger.trace("Some code inside a lambda, ignore"));
            fail("Expected Assertion Error to be thrown, but did not happen");
        } catch (AssertionFailedError e) {
            assertThat(e.getMessage(), is("Expected [org.elasticsearch.ElasticsearchException] to be thrown, but nothing was thrown."));
        }
    }

    public void testThatAssertThrowsCorrectlyFailsOnWrongException() {
        try {
            assertThrows(ElasticsearchException.class, () -> { throw new IndexOutOfBoundsException(); } );
        } catch (AssertionFailedError e) {
            assertThat(e.getMessage(), is("Unexpected exception type thrown, expected [org.elasticsearch.ElasticsearchException], got " +
                    "[java.lang.IndexOutOfBoundsException]"));
        }
    }
}
