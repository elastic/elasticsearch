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
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.type.CompactInvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.TypeConflictedField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ConvertFunction;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * A field mapped to a type ES|QL supports in some indices and to one it doesn't in others keeps its unsupported types, as long as every
 * node supports lenient casts: a cast of the field is {@code null} for them. Otherwise, as before, the field is unsupported.
 */
public class IndexResolverUnsupportedUnionTypesTests extends ESTestCase {

    public void testUnsupportedTypeInAUnion() {
        EsField field = resolve(TransportVersion.current(), index("idx-long", "long"), index("idx-rank", "rank_feature"));

        assertThat(field, instanceOf(CompactInvalidMappedField.class));
        TypeConflictedField conflict = (TypeConflictedField) field;
        assertThat(conflict.types(), equalTo(Set.of(DataType.LONG, DataType.UNSUPPORTED)));
        assertThat(conflict.getTypesToIndices(), equalTo(Map.of("long", Set.of("idx-long"), "unsupported", Set.of("idx-rank"))));
        assertThat(conflict.unsupportedOriginalTypesToIndices(), equalTo(Map.of("rank_feature", Set.of("idx-rank"))));
    }

    /**
     * The message of the conflict names the original unsupported types, not {@code unsupported}.
     */
    public void testErrorMessageNamesTheOriginalTypes() {
        TypeConflictedField conflict = (TypeConflictedField) resolve(
            TransportVersion.current(),
            index("idx-long", "long"),
            index("idx-rank", "rank_feature"),
            index("idx-rank-2", "rank_features")
        );

        assertThat(conflict.errorMessage(), containsString("[rank_feature] in [idx-rank]"));
        assertThat(conflict.errorMessage(), containsString("[rank_features] in [idx-rank-2]"));
        assertThat(conflict.errorMessage(), containsString("[long] in [idx-long]"));
    }

    /**
     * Below the version that supports lenient casts, a node can't take part in the casts of such a union: as before, the field is
     * unsupported, with the unsupported type it has.
     */
    public void testUnsupportedWhenSomeNodeDoesNotSupportLenientCasts() {
        EsField field = resolve(
            TransportVersionUtils.randomVersionNotSupporting(ConvertFunction.LENIENT_CAST),
            index("idx-long", "long"),
            index("idx-rank", "rank_feature")
        );

        assertThat(field, instanceOf(UnsupportedEsField.class));
        assertThat(((UnsupportedEsField) field).getOriginalTypes(), equalTo(List.of("rank_feature")));
    }

    /**
     * A field with only unsupported types is unsupported, whatever the version, with the first original type as always.
     */
    public void testOnlyUnsupportedTypes() {
        EsField field = resolve(TransportVersion.current(), index("idx-a", "rank_feature"), index("idx-b", "rank_feature"));

        assertThat(field, instanceOf(UnsupportedEsField.class));
        assertThat(((UnsupportedEsField) field).getOriginalTypes(), equalTo(List.of("rank_feature")));
    }

    private static EsField resolve(TransportVersion minimumVersion, FieldCapabilitiesIndexResponse... indices) {
        FieldCapabilitiesResponse caps = FieldCapabilitiesResponse.builder().withIndexResponses(List.of(indices)).build();
        var resolution = IndexResolver.mergedMappings(
            "idx-*",
            false,
            new IndexResolver.FieldsInfo(caps, minimumVersion, false, false, false, false, false, false),
            false,
            IndexResolver.DO_NOT_GROUP
        );
        return resolution.get().mapping().get("f");
    }

    private static FieldCapabilitiesIndexResponse index(String index, String type) {
        var f = new IndexFieldCapabilitiesBuilder("f", type).build();
        return new FieldCapabilitiesIndexResponse(index, index, Map.of("f", f), true, IndexMode.STANDARD, 1, 0, 1L, 1L, null);
    }
}
