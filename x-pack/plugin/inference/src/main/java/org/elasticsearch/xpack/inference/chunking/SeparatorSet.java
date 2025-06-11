/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import java.util.List;
import java.util.Locale;

public enum SeparatorSet {
    PLAINTEXT("plaintext"),
    MARKDOWN("markdown");

    private final String name;

    SeparatorSet(String name) {
        this.name = name;
    }

    public static SeparatorSet fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public List<String> getSeparators() {
        return switch (this) {
            case PLAINTEXT -> List.of("\n\n", "\n");
            case MARKDOWN -> List.of(
                "\n# ",
                "\n## ",
                "\n### ",
                "\n#### ",
                "\n##### ",
                "\n###### ",
                "^(?!\\s*$).*\\n*{3,}\\n",
                "^(?!\\s*$).*\\n-{3,}\\n",
                "^(?!\\s*$).*\\n_{3,}\\n"
            );
        };
    }
}
