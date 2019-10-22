package org.elasticsearch.xpack.security.authc.saml;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class NamedFormatterTests {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void patternAreFormatted() {
        assertThat(NamedFormatter.format("Hello, %(name)!", Map.of("name", "world")), equalTo("Hello, world!"));
    }

    @Test
    public void duplicatePatternsAreFormatted() {
        assertThat(NamedFormatter.format("Hello, %(name) and %(name)!", Map.of("name", "world")), equalTo("Hello, world and world!"));
    }

    @Test
    public void multiplePatternsAreFormatted() {
        assertThat(
            NamedFormatter.format("Hello, %(name) and %(second_name)!", Map.of("name", "world", "second_name", "fred")),
            equalTo("Hello, world and fred!")
        );
    }

    @Test
    public void escapedPatternsAreNotFormatted() {
        assertThat(NamedFormatter.format("Hello, \\%(name)!", Map.of("name", "world")), equalTo("Hello, %(name)!"));
    }

    @Test
    public void unknownPatternsThrowException() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("No parameter value for %(name)");
        NamedFormatter.format("Hello, %(name)!", Map.of("foo", "world"));
    }
}
