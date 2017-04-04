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
package org.elasticsearch.test.rest.yaml.section;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.junit.Assert.assertThat;

/**
 * Represents an is_false assert section:
 *
 *   - is_false:  get.fields.bar
 *
 */
public class IsFalseAssertion extends Assertion {
    public static IsFalseAssertion parse(XContentParser parser) throws IOException {
        return new IsFalseAssertion(parser.getTokenLocation(), ParserUtils.parseField(parser));
    }

    private static final Logger logger = Loggers.getLogger(IsFalseAssertion.class);

    public IsFalseAssertion(XContentLocation location, String field) {
        super(location, field, false);
    }

    @Override
    protected void doAssert(Object actualValue, Object expectedValue) {
        logger.trace("assert that [{}] doesn't have a true value (field: [{}])", actualValue, getField());

        if (actualValue == null) {
            return;
        }

        String actualString = actualValue.toString();
        assertThat(errorMessage(), actualString, anyOf(
                equalTo(""),
                equalToIgnoringCase(Boolean.FALSE.toString()),
                equalTo("0")
        ));
    }

    private String errorMessage() {
        return "field [" + getField() + "] has a true value but it shouldn't";
    }
}
