/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.initialization;

import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.entitlement.initialization.DynamicInstrumentationUtils.Descriptor;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.List;

import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class DynamicInstrumentationTests extends ESTestCase {

    public void testInstrumentedMethodsExist() throws Exception {
        List<Descriptor> descriptors = DynamicInstrumentationUtils.loadInstrumentedMethodDescriptors();
        assertThat(
            descriptors,
            everyItem(
                anyOf(
                    isKnownDescriptor(),
                    // not visible
                    transformedMatch(Descriptor::className, startsWith("jdk/vm/ci/services/")),
                    // removed in JDK 24
                    is(
                        new Descriptor(
                            "sun/net/www/protocol/http/HttpURLConnection",
                            "openConnectionCheckRedirects",
                            List.of("java/net/URLConnection")
                        )
                    ),
                    // removed in JDK 26
                    is(new Descriptor("java/net/MulticastSocket", "send", List.of("java/net/DatagramPacket", "B")))
                )
            )
        );
    }

    static Matcher<Descriptor> isKnownDescriptor() {
        return new TypeSafeMatcher<>() {
            @Override
            protected boolean matchesSafely(Descriptor item) {
                return Strings.isNotEmpty(item.className()) && Strings.isNotEmpty(item.methodName());
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("valid method descriptor");
            }

            @Override
            protected void describeMismatchSafely(Descriptor item, Description mismatchDescription) {
                mismatchDescription.appendText("was ").appendValue(item.toString());
            }
        };
    }
}
