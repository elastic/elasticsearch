/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        SkipSection skipSection = SkipSection.parseIfNext(parser);
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
        return new TeardownSection(skipSection, executableSections);
    }

    public static final TeardownSection EMPTY = new TeardownSection(SkipSection.EMPTY, Collections.emptyList());

    private final SkipSection skipSection;
    private final List<ExecutableSection> doSections;

    TeardownSection(SkipSection skipSection, List<ExecutableSection> doSections) {
        this.skipSection = Objects.requireNonNull(skipSection, "skip section cannot be null");
        this.doSections = Collections.unmodifiableList(doSections);
    }

    public SkipSection getSkipSection() {
        return skipSection;
    }

    public List<ExecutableSection> getDoSections() {
        return doSections;
    }

    public boolean isEmpty() {
        return EMPTY.equals(this);
    }
}
