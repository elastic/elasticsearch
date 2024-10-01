/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a test section, which is composed of a skip section and multiple executable sections.
 */
public class ClientYamlTestSection implements Comparable<ClientYamlTestSection> {
    public static ClientYamlTestSection parse(XContentParser parser) throws IOException {
        ParserUtils.advanceToFieldName(parser);
        XContentLocation sectionLocation = parser.getTokenLocation();
        String sectionName = parser.currentName();
        List<ExecutableSection> executableSections = new ArrayList<>();
        try {
            parser.nextToken();
            PrerequisiteSection prerequisiteSection = PrerequisiteSection.parseIfNext(parser);
            while (parser.currentToken() != XContentParser.Token.END_ARRAY) {
                ParserUtils.advanceToFieldName(parser);
                executableSections.add(ExecutableSection.parse(parser));
            }
            if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                throw new IllegalArgumentException(
                    "malformed section ["
                        + sectionName
                        + "] expected ["
                        + XContentParser.Token.END_OBJECT
                        + "] but was ["
                        + parser.currentToken()
                        + "]"
                );
            }
            parser.nextToken();
            return new ClientYamlTestSection(sectionLocation, sectionName, prerequisiteSection, executableSections);
        } catch (Exception e) {
            throw new ParsingException(parser.getTokenLocation(), "Error parsing test named [" + sectionName + "]", e);
        }
    }

    private final XContentLocation location;
    private final String name;
    private final PrerequisiteSection prerequisiteSection;
    private final List<ExecutableSection> executableSections;

    public ClientYamlTestSection(
        XContentLocation location,
        String name,
        PrerequisiteSection prerequisiteSection,
        List<ExecutableSection> executableSections
    ) {
        this.location = location;
        this.name = name;
        this.prerequisiteSection = Objects.requireNonNull(prerequisiteSection, "skip section cannot be null");
        this.executableSections = Collections.unmodifiableList(executableSections);
    }

    public XContentLocation getLocation() {
        return location;
    }

    public String getName() {
        return name;
    }

    public PrerequisiteSection getPrerequisiteSection() {
        return prerequisiteSection;
    }

    public List<ExecutableSection> getExecutableSections() {
        return executableSections;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClientYamlTestSection that = (ClientYamlTestSection) o;

        if (name != null ? name.equals(that.name) == false : that.name != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    @Override
    public int compareTo(ClientYamlTestSection o) {
        return name.compareTo(o.getName());
    }
}
