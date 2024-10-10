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
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TeardownSection {
    /**
     * Parse a {@link TeardownSection} if the next field is {@code skip}, otherwise returns {@link TeardownSection#EMPTY}.
     */
    static TeardownSection parseIfNext(XContentParser parser) throws IOException {
        ParserUtils.advanceToFieldName(parser);

        if ("teardown".equals(parser.currentName())) {
            parser.nextToken();
            TeardownSection section = parse(parser);
            parser.nextToken();
            return section;
        }

        return EMPTY;
    }

    public static TeardownSection parse(XContentParser parser) throws IOException {
        PrerequisiteSection prerequisiteSection = PrerequisiteSection.parseIfNext(parser);
        List<ExecutableSection> executableSections = new ArrayList<>();
        while (parser.currentToken() != XContentParser.Token.END_ARRAY) {
            ParserUtils.advanceToFieldName(parser);
            if ("do".equals(parser.currentName()) == false) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "section [" + parser.currentName() + "] not supported within teardown section"
                );
            }
            executableSections.add(DoSection.parse(parser));
            parser.nextToken();
        }

        parser.nextToken();
        return new TeardownSection(prerequisiteSection, executableSections);
    }

    public static final TeardownSection EMPTY = new TeardownSection(PrerequisiteSection.EMPTY, Collections.emptyList());

    private final PrerequisiteSection prerequisiteSection;
    private final List<ExecutableSection> doSections;

    TeardownSection(PrerequisiteSection prerequisiteSection, List<ExecutableSection> doSections) {
        this.prerequisiteSection = Objects.requireNonNull(prerequisiteSection, "skip section cannot be null");
        this.doSections = Collections.unmodifiableList(doSections);
    }

    public PrerequisiteSection getPrerequisiteSection() {
        return prerequisiteSection;
    }

    public List<ExecutableSection> getDoSections() {
        return doSections;
    }

    public boolean isEmpty() {
        return EMPTY.equals(this);
    }
}
