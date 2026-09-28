/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.update;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.translog.Translog;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DocValuesUpdateFastPathTests extends MapperServiceTestCase {

    private static Settings columnar() {
        // Updatable doc values require sequence numbers, which columnar disables by default.
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put("index.disable_sequence_numbers", false)
            .build();
    }

    private MappingLookup updatableLongMapping() throws Exception {
        return createMapperService(
            columnar(),
            fieldMapping(b -> b.field("type", "long").field("index", false).startObject("doc_values").field("updatable", true).endObject())
        ).mappingLookup();
    }

    /**
     * The in-place doc-values fast path is only taken when every node in the cluster understands the operation. Even with a valid
     * updatable-field partial document, the fast path is declined (so the update falls back to read-modify-reindex) when the operation is
     * not supported cluster-wide, and taken when it is. This pins the mixed-version safety gate.
     */
    public void testFastPathGatedOnClusterSupport() throws Exception {
        assumeTrue("doc_values updatable feature flag must be enabled", FieldMapper.DOC_VALUES_UPDATABLE_FEATURE_FLAG.isEnabled());
        MappingLookup mappingLookup = updatableLongMapping();
        UpdateRequest request = new UpdateRequest("idx", "1").doc(Map.of("field", 42));

        assertThat(
            "not supported cluster-wide: decline the in-place fast path",
            UpdateHelper.buildDocValuesFieldUpdates(request, mappingLookup, false),
            nullValue()
        );

        List<Translog.DocValuesUpdate.FieldUpdate> updates = UpdateHelper.buildDocValuesFieldUpdates(request, mappingLookup, true);
        assertThat("supported cluster-wide: take the in-place fast path", updates, notNullValue());
        assertThat(updates, hasSize(1));
        assertThat(updates.get(0).field(), equalTo("field"));
    }
}
