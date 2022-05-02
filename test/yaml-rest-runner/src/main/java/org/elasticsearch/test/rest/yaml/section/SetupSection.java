/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        SkipSection skipSection = SkipSection.parseIfNext(parser);
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
        return new SetupSection(skipSection, executableSections);
    }

    public static final SetupSection EMPTY = new SetupSection(SkipSection.EMPTY, Collections.emptyList());

    private final SkipSection skipSection;
    private final List<ExecutableSection> executableSections;

    public SetupSection(SkipSection skipSection, List<ExecutableSection> executableSections) {
        this.skipSection = Objects.requireNonNull(skipSection, "skip section cannot be null");
        this.executableSections = Collections.unmodifiableList(executableSections);
    }

    public SkipSection getSkipSection() {
        return skipSection;
    }

    public List<ExecutableSection> getExecutableSections() {
        return executableSections;
    }

    public boolean isEmpty() {
        return EMPTY.equals(this);
    }
}
