/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a setup section. Holds a skip section and multiple do sections.
 */
public class SetupSection {
    /**
     * Parse a {@link SetupSection} if the next field is {@code skip}, otherwise returns {@link SetupSection#EMPTY}.
     */
    static SetupSection parseIfNext(XContentParser parser) throws IOException {
        ParserUtils.advanceToFieldName(parser);

        if ("setup".equals(parser.currentName())) {
            parser.nextToken();
            SetupSection section = parse(parser);
            parser.nextToken();
            return section;
        }

        return EMPTY;
    }

    public static SetupSection parse(XContentParser parser) throws IOException {
        PrerequisiteSection prerequisiteSection = PrerequisiteSection.parseIfNext(parser);
        List<ExecutableSection> executableSections = new ArrayList<>();
        while (parser.currentToken() != XContentParser.Token.END_ARRAY) {
            ParserUtils.advanceToFieldName(parser);
            if ("do".equals(parser.currentName())) {
                executableSections.add(DoSection.parse(parser));
            } else if ("set".equals(parser.currentName())) {
                executableSections.add(SetSection.parse(parser));
            } else {
                throw new IllegalArgumentException("section [" + parser.currentName() + "] not supported within setup section");
            }

            parser.nextToken();
        }
        parser.nextToken();
        return new SetupSection(prerequisiteSection, executableSections);
    }

    public static final SetupSection EMPTY = new SetupSection(PrerequisiteSection.EMPTY, Collections.emptyList());

    private final PrerequisiteSection prerequisiteSection;
    private final List<ExecutableSection> executableSections;

    public SetupSection(PrerequisiteSection prerequisiteSection, List<ExecutableSection> executableSections) {
        this.prerequisiteSection = Objects.requireNonNull(prerequisiteSection, "skip section cannot be null");
        this.executableSections = Collections.unmodifiableList(executableSections);
    }

    public PrerequisiteSection getPrerequisiteSection() {
        return prerequisiteSection;
    }

    public List<ExecutableSection> getExecutableSections() {
        return executableSections;
    }

    public boolean isEmpty() {
        return EMPTY.equals(this);
    }
}
