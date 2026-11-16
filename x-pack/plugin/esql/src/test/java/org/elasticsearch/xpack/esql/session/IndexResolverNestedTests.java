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
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilities;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilitiesBuilder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.core.type.TypeConflictedField;
import org.elasticsearch.xpack.esql.index.NestedLeaves;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * A field under a nested object is a column only where every node reads it flattened; field caps no longer leaves it out.
 */
public class IndexResolverNestedTests extends ESTestCase {

    public void testFieldUnderNestedObjectIsAColumn() {
        assumeTrue("snapshot builds read the fields under a nested object", NestedLeaves.ENABLED);
        Map<String, EsField> mapping = resolve(TransportVersion.current(), nestedIndex());
        EsField pid = mapping.get("process").getProperties().get("pid");
        assertThat(pid.getDataType(), equalTo(DataType.LONG));
        // Each object holds one pid, but the parent's column holds those of all of its objects
        assertThat(pid.isStaticallySingleValued(), equalTo(false));
        assertThat(pid.isAggregatable(), equalTo(true));
        assertThat(pid.isUnderNested(), equalTo(true));
        EsField note = mapping.get("process").getProperties().get("note");
        assertThat(note, instanceOf(TextEsField.class));
        assertThat(note.isUnderNested(), equalTo(true));
        assertThat(mapping.get("name").getDataType(), equalTo(DataType.KEYWORD));
        assertThat(mapping.get("name").isUnderNested(), equalTo(false));
    }

    /**
     * Where one index maps the name under a nested object and another under a plain one, the values of the first are still
     * in its child documents, so the column can't be searched as a Lucene query on the parents.
     */
    public void testFieldUnderNestedObjectInOneIndexIsUnderNested() {
        assumeTrue("snapshot builds read the fields under a nested object", NestedLeaves.ENABLED);
        Map<String, EsField> mapping = resolve(
            TransportVersion.current(),
            nestedIndex(),
            index(
                "plain",
                new IndexFieldCapabilitiesBuilder("process", "object").build(),
                new IndexFieldCapabilitiesBuilder("process.pid", "long").build(),
                new IndexFieldCapabilitiesBuilder("process.note", "text").build()
            )
        );
        assertThat(mapping.get("process").getProperties().get("pid").isUnderNested(), equalTo(true));
        assertThat(mapping.get("process").getProperties().get("note").isUnderNested(), equalTo(true));
    }

    /**
     * A field the indices map under a nested object with different types stays under it, as does what it becomes where some
     * index doesn't map it, so that a data node reads its children once a cast resolves it.
     */
    public void testFieldUnderNestedObjectKeepsItAsATypeConflictOrPartiallyUnmapped() {
        assumeTrue("snapshot builds read the fields under a nested object", NestedLeaves.ENABLED);
        Map<String, EsField> mapping = resolve(
            TransportVersion.current(),
            nestedIndex(),
            index(
                "other",
                new IndexFieldCapabilitiesBuilder("process", "nested").build(),
                new IndexFieldCapabilitiesBuilder("process.pid", "keyword").build()
            )
        );
        EsField pid = mapping.get("process").getProperties().get("pid");
        assertThat(pid, instanceOf(TypeConflictedField.class));
        assertThat(pid.isUnderNested(), equalTo(true));
        assertThat(IndexResolver.wrapPartiallyUnmappedField(pid, "pid", "process.pid", Set.of("test")).isUnderNested(), equalTo(true));

        EsField keyword = new KeywordEsField("note", Map.of(), false, 0, false, false, EsField.TimeSeriesFieldType.NONE, false, true);
        EsField wrapped = IndexResolver.wrapPartiallyUnmappedField(keyword, "note", "process.note", Set.of("test"));
        assertThat(wrapped, instanceOf(PotentiallyUnmappedKeywordEsField.class));
        assertThat(wrapped.isUnderNested(), equalTo(true));
    }

    /**
     * An older data node can't read the children, so it would load null for a column a newer one fills. Every coordinator
     * leaves the field out, as field caps did before.
     */
    public void testWithAnOlderNodeFieldUnderNestedObjectIsLeftOut() {
        TransportVersion older = TransportVersionUtils.getPreviousVersion(NestedLeaves.VERSION);
        Map<String, EsField> mapping = resolve(older, nestedIndex());
        assertThat(mapping.get("process"), nullValue());
        assertThat(mapping.get("name").getDataType(), equalTo(DataType.KEYWORD));
    }

    private static FieldCapabilitiesIndexResponse nestedIndex() {
        return index(
            "test",
            new IndexFieldCapabilitiesBuilder("process", "nested").build(),
            new IndexFieldCapabilitiesBuilder("process.pid", "long").isStaticallySingleValued(true).build(),
            new IndexFieldCapabilitiesBuilder("process.note", "text").build(),
            new IndexFieldCapabilitiesBuilder("name", "keyword").build()
        );
    }

    private static Map<String, EsField> resolve(TransportVersion minimumVersion, FieldCapabilitiesIndexResponse... indices) {
        FieldCapabilitiesResponse caps = FieldCapabilitiesResponse.builder().withIndexResponses(List.of(indices)).build();
        return IndexResolver.mergedMappings(
            "*",
            false,
            new IndexResolver.FieldsInfo(caps, minimumVersion, false, false, false, false, false, false),
            false,
            IndexResolver.DO_NOT_GROUP
        ).get().mapping();
    }

    private static FieldCapabilitiesIndexResponse index(String index, IndexFieldCapabilities... fields) {
        Map<String, IndexFieldCapabilities> byName = new HashMap<>();
        for (IndexFieldCapabilities field : fields) {
            byName.put(field.name(), field);
        }
        return new FieldCapabilitiesIndexResponse(index, index, byName, true, IndexMode.STANDARD, 1, 0, 1L, 1L);
    }
}
