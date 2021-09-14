/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;

import java.io.IOException;
import java.util.List;

/**
 * Represents a test fragment that can be executed (e.g. api call, assertion)
 */
public interface ExecutableSection {
    /**
     * Default list of {@link ExecutableSection}s available for tests.
     */
    List<NamedXContentRegistry.Entry> DEFAULT_EXECUTABLE_CONTEXTS = List.of(
            new NamedXContentRegistry.Entry(ExecutableSection.class, new ParseField("do"), DoSection::parse),
            new NamedXContentRegistry.Entry(ExecutableSection.class, new ParseField("set"), SetSection::parse),
            new NamedXContentRegistry.Entry(ExecutableSection.class, new ParseField("transform_and_set"), TransformAndSetSection::parse),
            new NamedXContentRegistry.Entry(ExecutableSection.class, new ParseField("match"), MatchAssertion::parse),
            new NamedXContentRegistry.Entry(ExecutableSection.class, new ParseField("is_true"), IsTrueAssertion::parse),
            new NamedXContentRegistry.Entry(ExecutableSection.class, new ParseField("is_false"), IsFalseAssertion::parse),
            new NamedXContentRegistry.Entry(ExecutableSection.class, new ParseField("gt"), GreaterThanAssertion::parse),
            new NamedXContentRegistry.Entry(ExecutableSection.class, new ParseField("gte"), GreaterThanEqualToAssertion::parse),
            new NamedXContentRegistry.Entry(ExecutableSection.class, new ParseField("lt"), LessThanAssertion::parse),
            new NamedXContentRegistry.Entry(ExecutableSection.class, new ParseField("lte"), LessThanOrEqualToAssertion::parse),
            new NamedXContentRegistry.Entry(ExecutableSection.class, new ParseField("contains"), ContainsAssertion::parse),
            new NamedXContentRegistry.Entry(ExecutableSection.class, new ParseField("length"), LengthAssertion::parse),
            new NamedXContentRegistry.Entry(ExecutableSection.class, new ParseField("close_to"), CloseToAssertion::parse));

    /**
     * {@link NamedXContentRegistry} that parses the default list of
     * {@link ExecutableSection}s available for tests.
     */
    NamedXContentRegistry XCONTENT_REGISTRY = new NamedXContentRegistry(DEFAULT_EXECUTABLE_CONTEXTS);

    static ExecutableSection parse(XContentParser parser) throws IOException {
        ParserUtils.advanceToFieldName(parser);
        String section = parser.currentName();
        XContentLocation location = parser.getTokenLocation();
        try {
            ExecutableSection executableSection = parser.namedObject(ExecutableSection.class, section, null);
            parser.nextToken();
            return executableSection;
        } catch (Exception e) {
            throw new IOException("Error parsing section starting at [" + location + "]", e);
        }
    }

    /**
     * Get the location in the test that this was defined.
     */
    XContentLocation getLocation();

    /**
     * Executes the section passing in the execution context
     */
    void execute(ClientYamlTestExecutionContext executionContext) throws IOException;
}
