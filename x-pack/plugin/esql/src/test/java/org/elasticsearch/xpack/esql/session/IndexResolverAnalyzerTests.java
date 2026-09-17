/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilitiesBuilder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.index.IndexResolution;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class IndexResolverAnalyzerTests extends ESTestCase {

    /**
     * Shared analyzer name is kept only when every index reports the same one. A disagreement or a missing
     * name on either side returns null. HIGHLIGHT treats that as {@code standard}.
     */
    public void testSharedIndexAnalyzerNeedsEveryIndexToAgree() {
        assertThat(resolveTitle("english", "english").analyzerName(), equalTo("english"));
        assertThat(resolveTitle("english", "standard").analyzerName(), nullValue());
        assertThat(resolveTitle("english", null).analyzerName(), nullValue());
        assertThat(resolveTitle(null, "english").analyzerName(), nullValue());
    }

    private static TextEsField resolveTitle(String first, String second) {
        FieldCapabilitiesResponse caps = FieldCapabilitiesResponse.builder()
            .withIndexResponses(List.of(index("idx-a", first), index("idx-b", second)))
            .build();
        IndexResolution resolution = IndexResolver.mergedMappings(
            "idx-*",
            false,
            new IndexResolver.FieldsInfo(caps, TransportVersion.current(), false, false, false, false, false),
            false,
            IndexResolver.DO_NOT_GROUP
        );
        var esField = resolution.get().mapping().get("title");
        assertThat(esField, instanceOf(TextEsField.class));
        return (TextEsField) esField;
    }

    private static FieldCapabilitiesIndexResponse index(String index, String analyzer) {
        var title = new IndexFieldCapabilitiesBuilder("title", "text").indexAnalyzer(analyzer).build();
        return new FieldCapabilitiesIndexResponse(index, index, Map.of("title", title), true, IndexMode.STANDARD);
    }
}
