/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class ExternalSourceResolutionTests extends ESTestCase {

    /** A resolved CSV source whose metadata carries {@code config}. */
    public static ExternalSourceResolution.ResolvedSource source(Map<String, Object> config) {
        List<Attribute> schema = List.of(new ReferenceAttribute(Source.EMPTY, null, "a", DataType.KEYWORD));
        ExternalSourceMetadata metadata = new ExternalSourceMetadata() {
            @Override
            public String location() {
                return "file:///x.csv";
            }

            @Override
            public List<Attribute> schema() {
                return schema;
            }

            @Override
            public String sourceType() {
                return "csv";
            }

            @Override
            public Map<String, Object> config() {
                return config;
            }
        };
        return new ExternalSourceResolution.ResolvedSource(metadata, FileList.UNRESOLVED, Map.of());
    }

    /** The evidence is dropped, and everything else about the source is kept. */
    public void testWithoutQuoteFreeEvidenceKeepsTheRest() {
        var resolved = source(Map.of(SourceMetadata.INFERRED_QUOTE_FREE_CONFIG_KEY, "true", "error_mode", "skip_row"));
        var stripped = resolved.withoutQuoteFreeEvidence();
        assertThat(stripped.metadata().config(), equalTo(Map.of("error_mode", "skip_row")));
        assertThat(stripped.metadata().schema(), equalTo(resolved.metadata().schema()));
        assertThat(stripped.metadata().sourceType(), equalTo("csv"));
        assertThat(stripped.metadata().location(), equalTo("file:///x.csv"));
        assertThat(stripped.fileList(), sameInstance(resolved.fileList()));
        // The original is not mutated: its config may be shared with a cache.
        assertTrue(resolved.metadata().config().containsKey(SourceMetadata.INFERRED_QUOTE_FREE_CONFIG_KEY));
    }

    public void testWithoutQuoteFreeEvidenceIsANoOpWithoutTheKey() {
        var resolved = source(Map.of("error_mode", "skip_row"));
        assertThat(resolved.withoutQuoteFreeEvidence(), sameInstance(resolved));
    }

    public void testResolutionAppliesToEverySource() {
        var withKey = source(Map.of(SourceMetadata.INFERRED_QUOTE_FREE_CONFIG_KEY, "true"));
        var resolution = new ExternalSourceResolution(Map.of("a", withKey, "b", withKey), List.of("warning"));
        var stripped = resolution.withoutQuoteFreeEvidence();
        assertThat(stripped.warnings(), equalTo(List.of("warning")));
        for (String path : List.of("a", "b")) {
            assertTrue(stripped.resolvedSource(path).metadata().config().isEmpty());
        }
    }
}
