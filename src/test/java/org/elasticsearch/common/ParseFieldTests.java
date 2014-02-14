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

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.util.EnumSet;

import static org.hamcrest.CoreMatchers.*;

public class ParseFieldTests extends ElasticsearchTestCase {

    public void testParse() {
        String[] values = new String[]{"foo_bar", "fooBar"};
        ParseField field = new ParseField(randomFrom(values));
        String[] deprecated = new String[]{"barFoo", "bar_foo"};
        ParseField withDepredcations = field.withDeprecation("Foobar", randomFrom(deprecated));
        assertThat(field, not(sameInstance(withDepredcations)));
        assertThat(field.match(randomFrom(values), ParseField.EMPTY_FLAGS), is(true));
        assertThat(field.match("foo bar", ParseField.EMPTY_FLAGS), is(false));
        assertThat(field.match(randomFrom(deprecated), ParseField.EMPTY_FLAGS), is(false));
        assertThat(field.match("barFoo", ParseField.EMPTY_FLAGS), is(false));


        assertThat(withDepredcations.match(randomFrom(values), ParseField.EMPTY_FLAGS), is(true));
        assertThat(withDepredcations.match("foo bar", ParseField.EMPTY_FLAGS), is(false));
        assertThat(withDepredcations.match(randomFrom(deprecated), ParseField.EMPTY_FLAGS), is(true));
        assertThat(withDepredcations.match("barFoo", ParseField.EMPTY_FLAGS), is(true));

        // now with strict mode
        EnumSet<ParseField.Flag> flags = EnumSet.of(ParseField.Flag.STRICT);
        assertThat(field.match(randomFrom(values), flags), is(true));
        assertThat(field.match("foo bar", flags), is(false));
        assertThat(field.match(randomFrom(deprecated), flags), is(false));
        assertThat(field.match("barFoo", flags), is(false));


        assertThat(withDepredcations.match(randomFrom(values), flags), is(true));
        assertThat(withDepredcations.match("foo bar", flags), is(false));
        try {
            withDepredcations.match(randomFrom(deprecated), flags);
            fail();
        } catch (ElasticsearchIllegalArgumentException ex) {

        }

        try {
            withDepredcations.match("barFoo", flags);
            fail();
        } catch (ElasticsearchIllegalArgumentException ex) {

        }


    }

}
