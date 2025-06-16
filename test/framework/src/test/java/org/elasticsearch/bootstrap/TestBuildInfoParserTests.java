/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.test.LambdaMatchers.transformedItemsMatch;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class TestBuildInfoParserTests extends ESTestCase {
    public void testSimpleParsing() throws IOException {

        var input = """
            {
                "component": "lang-painless",
                "locations": [
                    {
                        "representativeClass": "Location.class",
                        "module": "org.elasticsearch.painless"
                    },
                    {
                        "representativeClass": "org/objectweb/asm/AnnotationVisitor.class",
                        "module": "org.objectweb.asm"
                    },
                    {
                        "representativeClass": "org/antlr/v4/runtime/ANTLRErrorListener.class",
                        "module": "org.antlr.antlr4.runtime"
                    },
                    {
                        "representativeClass": "org/objectweb/asm/commons/AdviceAdapter.class",
                        "module": "org.objectweb.asm.commons"
                    }
                ]
            }
            """;

        try (var parser = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, input)) {
            var testInfo = TestBuildInfoParser.fromXContent(parser);
            assertThat(testInfo.component(), is("lang-painless"));
            assertThat(
                testInfo.locations(),
                transformedItemsMatch(
                    TestBuildInfoLocation::module,
                    contains("org.elasticsearch.painless", "org.objectweb.asm", "org.antlr.antlr4.runtime", "org.objectweb.asm.commons")
                )
            );

            assertThat(
                testInfo.locations(),
                transformedItemsMatch(
                    TestBuildInfoLocation::representativeClass,
                    contains(
                        "Location.class",
                        "org/objectweb/asm/AnnotationVisitor.class",
                        "org/antlr/v4/runtime/ANTLRErrorListener.class",
                        "org/objectweb/asm/commons/AdviceAdapter.class"
                    )
                )
            );
        }
    }
}
