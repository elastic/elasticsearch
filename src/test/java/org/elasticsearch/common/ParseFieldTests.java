/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.elasticsearch.common;

import org.apache.commons.lang3.ArrayUtils;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.EnumSet;

import static org.hamcrest.CoreMatchers.*;

public class ParseFieldTests extends ElasticsearchTestCase {

    @Test
    public void testParse() {
        String[] values = new String[]{"foo_bar", "fooBar"};
        ParseField field = new ParseField(randomFrom(values));
        String[] deprecated = new String[]{"barFoo", "bar_foo"};
        ParseField withDeprecations = field.withDeprecation("Foobar", randomFrom(deprecated));
        assertThat(field, not(sameInstance(withDeprecations)));
        assertThat(field.match(randomFrom(values), ParseField.EMPTY_FLAGS), is(true));
        assertThat(field.match("foo bar", ParseField.EMPTY_FLAGS), is(false));
        assertThat(field.match(randomFrom(deprecated), ParseField.EMPTY_FLAGS), is(false));
        assertThat(field.match("barFoo", ParseField.EMPTY_FLAGS), is(false));

        assertThat(withDeprecations.match(randomFrom(values), ParseField.EMPTY_FLAGS), is(true));
        assertThat(withDeprecations.match("foo bar", ParseField.EMPTY_FLAGS), is(false));
        assertThat(withDeprecations.match(randomFrom(deprecated), ParseField.EMPTY_FLAGS), is(true));
        assertThat(withDeprecations.match("barFoo", ParseField.EMPTY_FLAGS), is(true));

        // now with strict mode
        EnumSet<ParseField.Flag> flags = EnumSet.of(ParseField.Flag.STRICT);
        assertThat(field.match(randomFrom(values), flags), is(true));
        assertThat(field.match("foo bar", flags), is(false));
        assertThat(field.match(randomFrom(deprecated), flags), is(false));
        assertThat(field.match("barFoo", flags), is(false));

        assertThat(withDeprecations.match(randomFrom(values), flags), is(true));
        assertThat(withDeprecations.match("foo bar", flags), is(false));
        try {
            withDeprecations.match(randomFrom(deprecated), flags);
            fail();
        } catch (ElasticsearchIllegalArgumentException ex) {

        }

        try {
            withDeprecations.match("barFoo", flags);
            fail();
        } catch (ElasticsearchIllegalArgumentException ex) {

        }
    }

    @Test
    public void testAllDeprecated() {
        String[] values = new String[]{"like_text", "likeText"};

        boolean withDeprecatedNames = randomBoolean();
        String[] deprecated = new String[]{"text", "same_as_text"};
        String[] allValues = values;
        if (withDeprecatedNames) {
            allValues = ArrayUtils.addAll(values, deprecated);
        }

        ParseField field = new ParseField(randomFrom(values));
        if (withDeprecatedNames) {
            field = field.withDeprecation(deprecated);
        }
        field = field.withAllDeprecated("like");

        // strict mode off
        assertThat(field.match(randomFrom(allValues), ParseField.EMPTY_FLAGS), is(true));
        assertThat(field.match("not a field name", ParseField.EMPTY_FLAGS), is(false));

        // now with strict mode
        EnumSet<ParseField.Flag> flags = EnumSet.of(ParseField.Flag.STRICT);
        try {
            field.match(randomFrom(allValues), flags);
            fail();
        } catch (ElasticsearchIllegalArgumentException ex) {
        }
    }
}
