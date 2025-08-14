/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest.yaml.section;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.ESTestCase.assertBusy;

/**
 * Represents an eventually section, which will repeat all child sections
 * in a loop until they succeed, for up to 10 seconds. e.g.
 *
 *   - eventually:
 *     - do:
 *         indices.get_index_template:
 *         name: traces-apm@template
 *
 */
public class EventuallySection implements ExecutableSection {

    private static final Logger logger = LogManager.getLogger(EventuallySection.class);

    private final XContentLocation location;
    private final List<ExecutableSection> executableSections;

    public static EventuallySection parse(XContentParser parser) throws IOException {
        XContentLocation location = parser.getTokenLocation();
        parser.nextToken(); // skip "eventually"
        List<ExecutableSection> executableSections = new ArrayList<>();
        while (parser.currentToken() != XContentParser.Token.END_ARRAY) {
            ParserUtils.advanceToFieldName(parser);
            executableSections.add(ExecutableSection.parse(parser));
        }
        parser.nextToken();
        return new EventuallySection(location, executableSections);
    }

    public EventuallySection(XContentLocation location, List<ExecutableSection> executableSections) {
        this.location = location;
        this.executableSections = executableSections;
    }

    @Override
    public XContentLocation getLocation() {
        return location;
    }

    @Override
    public final void execute(ClientYamlTestExecutionContext executionContext) throws IOException {
        try {
            assertBusy(() -> {
                for (ExecutableSection section : executableSections) {
                    section.execute(executionContext);
                }
            });
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
